from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LoadSegment:
    start_s: int
    end_s: int
    start_rate: int
    end_rate: int


BURSTY_PROFILE = [
    LoadSegment(0, 60, 2_000, 2_000),
    LoadSegment(60, 120, 2_000, 10_000),
    LoadSegment(120, 180, 10_000, 10_000),
    LoadSegment(180, 210, 10_000, 1_000),
    LoadSegment(210, 240, 1_000, 1_000),
    LoadSegment(240, 270, 1_000, 5_000),
    LoadSegment(270, 300, 5_000, 5_000),
]

CYCLIC_PROFILE = [
    LoadSegment(0, 30, 1_000, 1_000),
    LoadSegment(30, 60, 5_000, 5_000),
    LoadSegment(60, 90, 10_000, 10_000),
    LoadSegment(90, 120, 500, 500),
    LoadSegment(120, 150, 3_000, 3_000),
]

CYCLIC_DURATION_SECONDS = 150


def _rate_from_segments(
    segments: list[LoadSegment], elapsed_s: float, fallback_rate: int
) -> int:
    for segment in segments:
        if segment.start_s <= elapsed_s < segment.end_s:
            if segment.start_rate == segment.end_rate:
                return segment.start_rate
            duration = max(1, segment.end_s - segment.start_s)
            progress = (elapsed_s - segment.start_s) / duration
            return int(segment.start_rate + progress * (segment.end_rate - segment.start_rate))
    return fallback_rate


def rate_for_elapsed(profile_name: str, elapsed_s: float, fallback_rate: int) -> int:
    if profile_name == "constant":
        return fallback_rate
    if profile_name == "bursty":
        return _rate_from_segments(BURSTY_PROFILE, elapsed_s, fallback_rate)
    if profile_name == "cyclic":
        cycle_elapsed = elapsed_s % CYCLIC_DURATION_SECONDS
        return _rate_from_segments(CYCLIC_PROFILE, cycle_elapsed, fallback_rate)
    return fallback_rate


def max_rate_for_profile(profile_name: str, fallback_rate: int) -> int:
    if profile_name == "bursty":
        return max(max(s.start_rate, s.end_rate) for s in BURSTY_PROFILE)
    if profile_name == "cyclic":
        return max(max(s.start_rate, s.end_rate) for s in CYCLIC_PROFILE)
    return fallback_rate
