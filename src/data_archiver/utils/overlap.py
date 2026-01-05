from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable, Mapping, Optional, Sequence
import re

_TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})")


@dataclass(frozen=True)
class Interval:
    start: float
    end: float


def parse_timestamp_from_mcap(path: str) -> float:
    m = _TS_RE.search(path)
    if not m:
        raise ValueError(f"no timestamp found in {path}")
    dt = datetime.strptime(m.group(1), "%Y-%m-%d-%H-%M-%S")
    return dt.replace(tzinfo=timezone.utc).timestamp()


def merge_intervals(intervals: Iterable[Interval], gap_tol_s: float = 0.0) -> list[Interval]:
    xs = sorted(intervals, key=lambda i: i.start)
    if not xs:
        return []
    out = [xs[0]]
    for cur in xs[1:]:
        last = out[-1]
        if cur.start <= last.end + gap_tol_s:  # overlap or "close enough"
            out[-1] = Interval(last.start, max(last.end, cur.end))
        else:
            out.append(cur)
    return out


def intersect_intervals(a: list[Interval], b: list[Interval]) -> list[Interval]:
    i = j = 0
    out: list[Interval] = []
    while i < len(a) and j < len(b):
        s = max(a[i].start, b[j].start)
        e = min(a[i].end, b[j].end)
        if s < e:
            out.append(Interval(s, e))
        if a[i].end < b[j].end:
            i += 1
        else:
            j += 1
    return out


def largest_contiguous_overlap(
    bag_intervals: Iterable[tuple[float, float]],
    video_intervals: Iterable[tuple[float, float]],
    *,
    gap_tol_s: float = 3.0,
) -> Optional[Interval]:
    bags = merge_intervals((Interval(s, e) for s, e in bag_intervals), gap_tol_s=gap_tol_s)
    vids = merge_intervals((Interval(s, e) for s, e in video_intervals), gap_tol_s=gap_tol_s)

    overlaps = intersect_intervals(bags, vids)
    overlaps = merge_intervals(overlaps, gap_tol_s=gap_tol_s)  # contiguous overlap with tolerated gaps
    return max(overlaps, key=lambda iv: iv.end - iv.start, default=None)


def intersects(a: Interval, b: Interval) -> bool:
    return a.start < b.end and b.start < a.end


def intervals_from_files(files: Sequence[str], duration_s: float) -> list[tuple[str, Interval]]:
    out: list[tuple[str, Interval]] = []
    for f in files:
        s = parse_timestamp_from_mcap(f)
        out.append((f, Interval(s, s + duration_s)))
    return out


def find_best_overlap(
    files: Sequence[str],
    *,
    bag_duration_s: float = 10 * 60,
    video_duration_s: float = 1 * 60,
    gap_tol_s: float = 5.0,
    use_mcap_timing: bool = False,
    mcap_timing_func: Callable[[str], tuple[float, float, float]] | None = None,
    mcap_timing_path_map: Mapping[str, str] | None = None,
) -> tuple[Optional[Interval], list[str], list[str]]:
    bag_files = [f for f in files if f.endswith(".bag") or (f.endswith(".mcap") and not f.endswith("_h264.mcap"))]
    video_files = [f for f in files if f.endswith("_h264.mcap")]

    if use_mcap_timing:
        if mcap_timing_func is None:
            raise ValueError("mcap_timing_func is required when use_mcap_timing=True")
        bag_items = []
        for f in bag_files:
            timing_path = mcap_timing_path_map.get(f, f) if mcap_timing_path_map else f
            start_s, end_s, _ = mcap_timing_func(timing_path)
            bag_items.append((f, Interval(start_s, end_s)))
        video_items = []
        for f in video_files:
            timing_path = mcap_timing_path_map.get(f, f) if mcap_timing_path_map else f
            start_s, end_s, _ = mcap_timing_func(timing_path)
            video_items.append((f, Interval(start_s, end_s)))
    else:
        bag_items = intervals_from_files(bag_files, bag_duration_s)
        video_items = intervals_from_files(video_files, video_duration_s)

    best = largest_contiguous_overlap(
        [(iv.start, iv.end) for _, iv in bag_items],
        [(iv.start, iv.end) for _, iv in video_items],
        gap_tol_s=gap_tol_s,
    )

    if best is None:
        return None, [], []

    bag_hits = [f for f, iv in bag_items if intersects(iv, best)]
    video_hits = [f for f, iv in video_items if intersects(iv, best)]
    return best, bag_hits, video_hits
