#!/usr/bin/env python3
"""
Build Science Bowl packets from a drafting spreadsheet.

Features:
- Parses .xlsx directly (no openpyxl dependency required)
- Builds RR and DE packets with subject + role constraints
- Balances packet difficulty with separate RR/DE targets
- Exports packet text files in a RR-style moderator format
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import textwrap
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

XML_NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pr": "http://schemas.openxmlformats.org/package/2006/relationships",
}

CORE_SUBJECTS = [
    "Biology",
    "Chemistry",
    "Physics",
    "Earth and Space",
    "Math and CS",
]
SPECIAL_SUBJECT = "BUMS Special"
DIFFICULTY_BUCKETS = ("RR1", "Easy", "Medium", "Hard", "DE7+")


@dataclass(frozen=True)
class Question:
    qid: str
    writer: str
    subject: str
    qtype: str
    prompt: str
    options: Tuple[str, str, str, str]
    answer: str
    difficulty_label: str
    difficulty_bucket: str
    difficulty_score: float
    roles: frozenset[str]
    sheet: str
    row_number: int


@dataclass
class Entry:
    role: str
    number: int
    question: Question


@dataclass
class Packet:
    name: str
    phase: str
    entries: List[Entry]


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\u200b", "").split()).strip()


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _difficulty_score(label: str) -> float:
    s = label.lower().strip()
    mapping = {
        "rr1": 1.0,
        "easy": 2.0,
        "medium": 3.0,
        "hard": 4.0,
        "de7+": 5.0,
    }
    if s in mapping:
        return mapping[s]
    numeric = _parse_float(s)
    if numeric is not None:
        return numeric
    for key, val in mapping.items():
        if key in s:
            return val
    return 3.0


def _difficulty_bucket(label: str) -> str:
    s = label.lower().replace(" ", "")
    if "rr1" in s:
        return "RR1"
    if "easy" in s:
        return "Easy"
    if "medium" in s:
        return "Medium"
    if "hard" in s:
        return "Hard"
    if "de7+" in s or "de7" in s:
        return "DE7+"
    return "Medium"


def _normalize_bucket_name(name: str) -> str:
    n = name.strip().lower().replace(" ", "")
    if n == "rr1":
        return "RR1"
    if n == "easy":
        return "Easy"
    if n == "medium":
        return "Medium"
    if n == "hard":
        return "Hard"
    if n in {"de7+", "de7"}:
        return "DE7+"
    raise ValueError(
        f"Unknown difficulty bucket '{name}'. Allowed: {', '.join(DIFFICULTY_BUCKETS)}."
    )


def _infer_subject(raw_subject: str, sheet_name: str) -> str:
    s = raw_subject.strip().upper()
    if s in {"B", "BIO", "BIOLOGY"}:
        return "Biology"
    if s in {"C", "CHEM", "CHEMISTRY"}:
        return "Chemistry"
    if s in {"P", "PHYS", "PHYSICS"}:
        return "Physics"
    if s in {"ES", "ESS", "EARTH", "EARTH AND SPACE"}:
        return "Earth and Space"
    if s in {"M", "MATH", "CS", "MATHCS", "MATH/CS", "MATH AND CS"}:
        return "Math and CS"
    if s in {"E", "SPECIAL", "BUMS SPECIAL", "BUMS SPECIALS"}:
        return SPECIAL_SUBJECT

    sheet = sheet_name.strip().upper()
    if sheet == "BIOLOGY":
        return "Biology"
    if sheet == "CHEMISTRY":
        return "Chemistry"
    if sheet == "PHYSICS":
        return "Physics"
    if sheet in {"ESS", "EARTH AND SPACE"}:
        return "Earth and Space"
    if sheet == "MATHCS":
        return "Math and CS"
    if sheet == "BUMS SPECIALS":
        return SPECIAL_SUBJECT
    return raw_subject.strip() or sheet_name.strip()


def _parse_roles(value: str) -> frozenset[str]:
    s = value.lower().strip()
    if not s:
        return frozenset({"TU", "Bonus"})
    if "both" in s:
        return frozenset({"TU", "Bonus"})
    roles: Set[str] = set()
    if "tu" in s or "toss" in s:
        roles.add("TU")
    if "bonus" in s:
        roles.add("Bonus")
    if not roles:
        return frozenset({"TU", "Bonus"})
    return frozenset(roles)


def _question_type_display(qtype: str) -> str:
    t = qtype.strip().upper()
    if t == "MC":
        return "Multiple Choice"
    return "Short Answer"


def _cell_value(
    cell: ET.Element,
    shared_strings: Sequence[str],
) -> str:
    cell_type = cell.attrib.get("t")
    v = cell.find("m:v", XML_NS)
    if v is None:
        inline = cell.find("m:is", XML_NS)
        if inline is None:
            return ""
        return "".join((n.text or "") for n in inline.findall(".//m:t", XML_NS))
    if cell_type == "s":
        if v.text is None:
            return ""
        idx = int(v.text)
        if 0 <= idx < len(shared_strings):
            return shared_strings[idx]
        return ""
    return v.text or ""


def _column_letters(cell_ref: str) -> str:
    return "".join(ch for ch in cell_ref if ch.isalpha())


def _col_to_idx(col: str) -> int:
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n


def _read_workbook_rows(xlsx_path: Path) -> Dict[str, List[Tuple[int, Dict[str, str]]]]:
    with zipfile.ZipFile(xlsx_path) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            sst = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in sst.findall("m:si", XML_NS):
                shared_strings.append(
                    "".join((t.text or "") for t in si.findall(".//m:t", XML_NS))
                )

        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels.findall("pr:Relationship", XML_NS)
        }

        result: Dict[str, List[Tuple[int, Dict[str, str]]]] = {}
        for sheet in wb.findall("m:sheets/m:sheet", XML_NS):
            name = sheet.attrib["name"]
            rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            target = rel_map[rid]
            path = target if target.startswith("xl/") else f"xl/{target}"
            root = ET.fromstring(zf.read(path))
            rows: List[Tuple[int, Dict[str, str]]] = []
            for row in root.findall(".//m:sheetData/m:row", XML_NS):
                row_num = int(row.attrib.get("r", "0"))
                row_vals: Dict[str, str] = {}
                for cell in row.findall("m:c", XML_NS):
                    ref = cell.attrib.get("r", "")
                    col = _column_letters(ref)
                    if not col:
                        continue
                    row_vals[col] = _cell_value(cell, shared_strings)
                rows.append((row_num, row_vals))
            result[name] = rows
        return result


def _extract_questions(
    workbook_rows: Dict[str, List[Tuple[int, Dict[str, str]]]],
    include_visual_bonuses: bool = False,
) -> List[Question]:
    skip_sheets = {"question stats"}
    if not include_visual_bonuses:
        skip_sheets.add("visual bonuses")

    questions: List[Question] = []
    for sheet_name, rows in workbook_rows.items():
        if sheet_name.lower() in skip_sheets:
            continue

        header_row_index = None
        header_map: Dict[int, str] = {}
        for _, row in rows[:20]:
            normalized = {
                _col_to_idx(col): _normalize_text(val).lower().replace("/", "")
                for col, val in row.items()
                if _normalize_text(val)
            }
            values = set(normalized.values())
            if "question" in values and "answer" in values:
                header_row_index = normalized
                break
        if header_row_index is None:
            continue

        for idx, name in header_row_index.items():
            header_map[idx] = name

        def get_col(row_dict: Dict[str, str], possible_headers: Iterable[str]) -> str:
            candidates = {h.lower().replace("/", "") for h in possible_headers}
            for col, val in row_dict.items():
                col_idx = _col_to_idx(col)
                header_name = header_map.get(col_idx, "")
                if header_name in candidates:
                    return _normalize_text(val)
            return ""

        for row_num, row in rows:
            prompt = get_col(row, {"question"})
            answer = get_col(row, {"answer"})
            if not prompt or not answer:
                continue

            writer = get_col(row, {"writer"})
            raw_subject = get_col(row, {"subject"})
            qtype = get_col(row, {"type"}) or "SA"
            difficulty = get_col(row, {"difficulty"}) or "Medium"
            role_text = get_col(row, {"tubonus", "tu bonus"})
            subject = _infer_subject(raw_subject, sheet_name)
            roles = _parse_roles(role_text)

            w = get_col(row, {"w1", "w", "1"})
            x = get_col(row, {"x2", "x", "2"})
            y = get_col(row, {"y3", "y", "3"})
            z = get_col(row, {"z4", "z", "4"})

            question = Question(
                qid=f"{sheet_name}!{row_num}",
                writer=writer,
                subject=subject,
                qtype=qtype,
                prompt=prompt,
                options=(w, x, y, z),
                answer=answer,
                difficulty_label=difficulty,
                difficulty_bucket=_difficulty_bucket(difficulty),
                difficulty_score=_difficulty_score(difficulty),
                roles=roles,
                sheet=sheet_name,
                row_number=row_num,
            )
            questions.append(question)

    return questions


def _pick_question(
    remaining: Dict[str, Question],
    requirements_remaining: Dict[Tuple[str, str], int],
    subject: str,
    role: str,
    target_difficulty: float,
    rng: random.Random,
    difficulty_needs: Dict[str, int] | None = None,
    strict_difficulty_plan: bool = True,
) -> Question:
    strict_tu = 0
    strict_bonus = 0
    both = 0
    for q in remaining.values():
        if q.subject != subject:
            continue
        if q.roles == frozenset({"TU"}):
            strict_tu += 1
        elif q.roles == frozenset({"Bonus"}):
            strict_bonus += 1
        elif q.roles == frozenset({"TU", "Bonus"}):
            both += 1

    candidates = [
        q
        for q in remaining.values()
        if q.subject == subject and role in q.roles
    ]
    if not candidates:
        raise ValueError(f"No remaining {role} question for subject '{subject}'.")

    feasible: List[Question] = []
    for c in candidates:
        after_strict_tu = strict_tu - (1 if c.roles == frozenset({"TU"}) else 0)
        after_strict_bonus = strict_bonus - (1 if c.roles == frozenset({"Bonus"}) else 0)
        after_both = both - (1 if c.roles == frozenset({"TU", "Bonus"}) else 0)
        need_tu = requirements_remaining.get((subject, "TU"), 0) - (1 if role == "TU" else 0)
        need_bonus = requirements_remaining.get((subject, "Bonus"), 0) - (
            1 if role == "Bonus" else 0
        )
        if need_tu <= after_strict_tu + after_both and need_bonus <= after_strict_bonus + after_both:
            feasible.append(c)

    if not feasible:
        feasible = candidates

    if difficulty_needs is not None:
        planned = [q for q in feasible if difficulty_needs.get(q.difficulty_bucket, 0) > 0]
        if planned:
            feasible = planned
        elif strict_difficulty_plan:
            raise ValueError(
                f"No remaining {role} question for subject '{subject}' that matches "
                f"the packet's outstanding difficulty quotas."
            )

    # Favor strict-role items first to preserve flexibility, then match difficulty.
    chosen = min(
        feasible,
        key=lambda q: (
            0 if q.roles == frozenset({role}) else 1,
            abs(q.difficulty_score - target_difficulty),
            rng.random() * 0.05,
        ),
    )
    del remaining[chosen.qid]
    return chosen


def _build_packets(
    questions: Sequence[Question],
    rr_rounds: int,
    de_rounds: int,
    per_subject: int,
    specials_per_packet: int,
    packet_targets: Sequence[float],
    raw_difficulty_plan: Dict[str, Dict[str, int]] | None,
    strict_difficulty_plan: bool,
    seed: int,
    max_attempts: int = 200,
) -> List[Packet]:
    total_packets = rr_rounds + de_rounds
    if len(packet_targets) != total_packets:
        raise ValueError(
            f"Expected {total_packets} packet targets, got {len(packet_targets)}."
        )
    base_required: Dict[Tuple[str, str], int] = {}
    for subject in CORE_SUBJECTS:
        base_required[(subject, "TU")] = total_packets * per_subject
        base_required[(subject, "Bonus")] = total_packets * per_subject
    if specials_per_packet > 0:
        per_packet_tu = (specials_per_packet + 1) // 2
        per_packet_bonus = specials_per_packet // 2
        base_required[(SPECIAL_SUBJECT, "TU")] = total_packets * per_packet_tu
        base_required[(SPECIAL_SUBJECT, "Bonus")] = total_packets * per_packet_bonus

    for (subject, role), need in base_required.items():
        have = sum(1 for q in questions if q.subject == subject and role in q.roles)
        if have < need:
            raise ValueError(
                f"Insufficient pool for {subject} {role}: need {need}, found {have}."
            )

    # Cycle subjects evenly (B, C, P, ES, M/CS, repeat...) to mirror standard packet flow.
    subject_sequence = [s for _ in range(per_subject) for s in CORE_SUBJECTS]
    last_error: Exception | None = None

    for attempt in range(max_attempts):
        remaining: Dict[str, Question] = {q.qid: q for q in questions}
        requirements_remaining = dict(base_required)
        rng = random.Random(seed + attempt)
        packets: List[Packet] = []

        def build_packet(name: str, phase: str, target: float) -> Packet:
            entries: List[Entry] = []
            total_questions_in_packet = len(subject_sequence) * 2 + specials_per_packet
            packet_quota = _resolve_packet_difficulty_quota(
                packet_name=name,
                phase=phase,
                total_questions_in_packet=total_questions_in_packet,
                raw_plan=raw_difficulty_plan,
            )
            for i, subject in enumerate(subject_sequence, start=1):
                tu = _pick_question(
                    remaining,
                    requirements_remaining,
                    subject,
                    "TU",
                    target,
                    rng,
                    difficulty_needs=packet_quota,
                    strict_difficulty_plan=strict_difficulty_plan,
                )
                requirements_remaining[(subject, "TU")] -= 1
                if packet_quota is not None:
                    packet_quota[tu.difficulty_bucket] -= 1
                bonus = _pick_question(
                    remaining,
                    requirements_remaining,
                    subject,
                    "Bonus",
                    target,
                    rng,
                    difficulty_needs=packet_quota,
                    strict_difficulty_plan=strict_difficulty_plan,
                )
                requirements_remaining[(subject, "Bonus")] -= 1
                if packet_quota is not None:
                    packet_quota[bonus.difficulty_bucket] -= 1
                entries.append(Entry(role="TU", number=i, question=tu))
                entries.append(Entry(role="Bonus", number=i, question=bonus))

            next_number = len(subject_sequence) + 1
            for i in range(specials_per_packet):
                role = "TU" if i % 2 == 0 else "Bonus"
                special = _pick_question(
                    remaining,
                    requirements_remaining,
                    SPECIAL_SUBJECT,
                    role,
                    target,
                    rng,
                    difficulty_needs=packet_quota,
                    strict_difficulty_plan=strict_difficulty_plan,
                )
                requirements_remaining[(SPECIAL_SUBJECT, role)] -= 1
                if packet_quota is not None:
                    packet_quota[special.difficulty_bucket] -= 1
                entries.append(Entry(role=role, number=next_number, question=special))
                next_number += 1
            if packet_quota is not None and strict_difficulty_plan:
                leftover = {k: v for k, v in packet_quota.items() if v != 0}
                if leftover:
                    raise ValueError(
                        f"Could not satisfy difficulty quota for {name}; leftover={leftover}."
                    )
            return Packet(name=name, phase=phase, entries=entries)

        try:
            idx = 0
            for i in range(1, rr_rounds + 1):
                packets.append(
                    build_packet(name=f"RR{i}", phase="RR", target=packet_targets[idx])
                )
                idx += 1
            for i in range(1, de_rounds + 1):
                packets.append(
                    build_packet(name=f"DE{i}", phase="DE", target=packet_targets[idx])
                )
                idx += 1
            return packets
        except ValueError as exc:
            last_error = exc
            continue

    raise ValueError(f"Unable to build packets after {max_attempts} attempts: {last_error}")


def _max_feasible_per_subject(questions: Sequence[Question], total_packets: int) -> int:
    limits: List[int] = []
    for subject in CORE_SUBJECTS:
        subject_q = [q for q in questions if q.subject == subject]
        unique_cap = len(subject_q)
        tu_cap = sum(1 for q in subject_q if "TU" in q.roles)
        bonus_cap = sum(1 for q in subject_q if "Bonus" in q.roles)
        max_pairs_total = min(unique_cap // 2, tu_cap, bonus_cap)
        limits.append(max_pairs_total // total_packets)
    return min(limits) if limits else 0


def _linear_targets(count: int, start: float, end: float) -> List[float]:
    if count <= 0:
        return []
    if count == 1:
        return [start]
    step = (end - start) / (count - 1)
    return [start + i * step for i in range(count)]


def _load_difficulty_plan(path: Path) -> Dict[str, Dict[str, int]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Difficulty plan must be a JSON object.")
    plan: Dict[str, Dict[str, int]] = {}
    for key, value in data.items():
        if not isinstance(key, str):
            raise ValueError("Difficulty plan keys must be strings.")
        if not isinstance(value, dict):
            raise ValueError(f"Difficulty plan for '{key}' must be an object.")
        bucket_counts: Dict[str, int] = {}
        for raw_bucket, raw_count in value.items():
            bucket = _normalize_bucket_name(str(raw_bucket))
            if not isinstance(raw_count, int) or raw_count < 0:
                raise ValueError(
                    f"Difficulty count for '{key}.{raw_bucket}' must be a non-negative integer."
                )
            bucket_counts[bucket] = bucket_counts.get(bucket, 0) + raw_count
        plan[key] = bucket_counts
    return plan


def _resolve_packet_difficulty_quota(
    packet_name: str,
    phase: str,
    total_questions_in_packet: int,
    raw_plan: Dict[str, Dict[str, int]] | None,
) -> Dict[str, int] | None:
    if raw_plan is None:
        return None
    quota: Dict[str, int] = {bucket: 0 for bucket in DIFFICULTY_BUCKETS}
    used = False
    for key in ("ALL", f"{phase}*", packet_name):
        if key not in raw_plan:
            continue
        used = True
        for bucket, count in raw_plan[key].items():
            quota[bucket] = count
    if not used:
        return None
    if sum(quota.values()) != total_questions_in_packet:
        raise ValueError(
            f"Difficulty plan for {packet_name} must sum to {total_questions_in_packet}, "
            f"got {sum(quota.values())}."
        )
    return quota


def _wrap(line: str, width: int = 88, indent: str = "") -> str:
    return textwrap.fill(line, width=width, subsequent_indent=indent)


def _format_question_block(entry: Entry) -> str:
    q = entry.question
    role_name = "TOSS-UP" if entry.role == "TU" else "BONUS"
    qtype = _question_type_display(q.qtype)
    lines = [role_name]
    lines.append(_wrap(f"{entry.number}) {q.subject} - {qtype} - {q.prompt}", indent="   "))

    if any(opt.strip() for opt in q.options):
        if q.qtype.strip().upper() == "MC":
            labels = ["W", "X", "Y", "Z"]
            for label, opt in zip(labels, q.options):
                if opt.strip():
                    lines.append(_wrap(f"{label}) {opt}", indent="   "))
        else:
            for idx, opt in enumerate(q.options, start=1):
                if opt.strip():
                    lines.append(_wrap(f"{idx}) {opt}", indent="   "))

    writer_suffix = f" [{q.writer}]" if q.writer else ""
    lines.append(_wrap(f"Answer: {q.answer}{writer_suffix}", indent="   "))
    return "\n".join(lines)


def _packet_stats(packet: Packet) -> Tuple[float, Dict[str, int]]:
    diffs = [e.question.difficulty_score for e in packet.entries]
    mean = sum(diffs) / len(diffs) if diffs else 0.0
    subject_counts: Dict[str, int] = {}
    for e in packet.entries:
        subject_counts[e.question.subject] = subject_counts.get(e.question.subject, 0) + 1
    return mean, subject_counts


def _packet_difficulty_counts(packet: Packet) -> Dict[str, int]:
    counts = {bucket: 0 for bucket in DIFFICULTY_BUCKETS}
    for e in packet.entries:
        counts[e.question.difficulty_bucket] += 1
    return counts


def _write_packet(packet: Packet, output_dir: Path, tournament_name: str) -> None:
    out_path = output_dir / f"{packet.name}.txt"
    lines = [f"{tournament_name}", packet.name, ""]
    for entry in packet.entries:
        lines.append(_format_question_block(entry))
        lines.append("")
    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _write_assignment_csv(packets: Sequence[Packet], output_dir: Path) -> None:
    out_path = output_dir / "assignments.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "packet",
                "phase",
                "role",
                "number",
                "qid",
                "sheet",
                "row",
                "subject",
                "difficulty",
                "difficulty_bucket",
                "difficulty_score",
                "writer",
            ]
        )
        for packet in packets:
            for e in packet.entries:
                q = e.question
                writer.writerow(
                    [
                        packet.name,
                        packet.phase,
                        e.role,
                        e.number,
                        q.qid,
                        q.sheet,
                        q.row_number,
                        q.subject,
                        q.difficulty_label,
                        q.difficulty_bucket,
                        q.difficulty_score,
                        q.writer,
                    ]
                )


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate Science Bowl RR/DE packets from XLSX.")
    p.add_argument("--input", required=True, help="Path to drafting workbook (.xlsx)")
    p.add_argument("--output-dir", default="generated_packets", help="Output directory")
    p.add_argument("--rr-rounds", type=int, default=6, help="Number of RR packets")
    p.add_argument("--de-rounds", type=int, default=10, help="Number of DE packets")
    p.add_argument(
        "--per-subject",
        type=int,
        default=4,
        help="Core TU+Bonus pairs per core subject per packet",
    )
    p.add_argument(
        "--specials-per-packet",
        type=int,
        default=0,
        help="Additional BUMS special questions per packet",
    )
    p.add_argument(
        "--rr-target-start",
        type=float,
        default=2.2,
        help="RR1 target difficulty",
    )
    p.add_argument(
        "--rr-target-end",
        type=float,
        default=2.9,
        help="RR(last) target difficulty",
    )
    p.add_argument(
        "--de-target-start",
        type=float,
        default=3.1,
        help="DE1 target difficulty",
    )
    p.add_argument(
        "--de-target-end",
        type=float,
        default=4.4,
        help="DE(last) target difficulty",
    )
    p.add_argument("--seed", type=int, default=42, help="Random seed for tie breaks")
    p.add_argument(
        "--difficulty-plan",
        help=(
            "Path to JSON difficulty distribution plan. Keys can be ALL, RR*, DE*, RR1..RRn, DE1..DEn. "
            "Values are bucket-count maps, e.g. {\"RR1\":12,\"Easy\":8,\"Medium\":20}."
        ),
    )
    p.add_argument(
        "--relaxed-difficulty-plan",
        action="store_true",
        help="Treat difficulty plan as preference instead of strict hard quotas.",
    )
    p.add_argument(
        "--include-visual-bonuses",
        action="store_true",
        help="Include 'visual bonuses' tab in parsing",
    )
    p.add_argument("--tournament-name", default="Science Bowl", help="Header name in packets")
    return p


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Workbook not found: {input_path}")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    workbook_rows = _read_workbook_rows(input_path)
    questions = _extract_questions(
        workbook_rows,
        include_visual_bonuses=args.include_visual_bonuses,
    )
    if not questions:
        raise ValueError("No valid questions were parsed from workbook.")

    total_packets = args.rr_rounds + args.de_rounds
    feasible_per_subject = _max_feasible_per_subject(questions, total_packets)
    per_subject = args.per_subject
    if per_subject <= 0:
        raise ValueError("--per-subject must be a positive integer.")
    if per_subject > feasible_per_subject:
        raise ValueError(
            f"Requested --per-subject={per_subject} is infeasible with current question pool. "
            f"Max feasible is {feasible_per_subject}."
        )

    rr_targets = _linear_targets(args.rr_rounds, args.rr_target_start, args.rr_target_end)
    de_targets = _linear_targets(args.de_rounds, args.de_target_start, args.de_target_end)
    packet_targets = rr_targets + de_targets
    raw_difficulty_plan = None
    if args.difficulty_plan:
        plan_path = Path(args.difficulty_plan).expanduser().resolve()
        if not plan_path.exists():
            raise FileNotFoundError(f"Difficulty plan not found: {plan_path}")
        raw_difficulty_plan = _load_difficulty_plan(plan_path)

    packets = _build_packets(
        questions=questions,
        rr_rounds=args.rr_rounds,
        de_rounds=args.de_rounds,
        per_subject=per_subject,
        specials_per_packet=args.specials_per_packet,
        packet_targets=packet_targets,
        raw_difficulty_plan=raw_difficulty_plan,
        strict_difficulty_plan=not args.relaxed_difficulty_plan,
        seed=args.seed,
    )

    for packet in packets:
        _write_packet(packet, output_dir, args.tournament_name)
    _write_assignment_csv(packets, output_dir)

    print(f"Parsed questions: {len(questions)}")
    print(f"Generated packets: {len(packets)}")
    print(f"Per subject pairs per packet: {per_subject}")
    if raw_difficulty_plan is not None:
        mode = "relaxed" if args.relaxed_difficulty_plan else "strict"
        print(f"Difficulty plan: enabled ({mode})")
    print(f"Output directory: {output_dir}")
    for i, packet in enumerate(packets):
        mean, subject_counts = _packet_stats(packet)
        diff_counts = _packet_difficulty_counts(packet)
        counts = ", ".join(f"{k}={v}" for k, v in sorted(subject_counts.items()))
        diff_str = ", ".join(f"{k}={v}" for k, v in diff_counts.items() if v > 0)
        target = packet_targets[i]
        print(
            f"- {packet.name}: target={target:.2f}, mean_difficulty={mean:.2f}; "
            f"{counts}; difficulties: {diff_str}"
        )


if __name__ == "__main__":
    main()
