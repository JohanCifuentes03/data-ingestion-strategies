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


def rate_for_elapsed(profile_name: str, elapsed_s: float, fallback_rate: int) -> int:
    if profile_name == "constant":
        return fallback_rate

    if profile_name != "bursty":
        return fallback_rate

    for segment in BURSTY_PROFILE:
        if segment.start_s <= elapsed_s < segment.end_s:
            if segment.start_rate == segment.end_rate:
                return segment.start_rate

            duration = max(1, segment.end_s - segment.start_s)
            progress = (elapsed_s - segment.start_s) / duration
            return int(segment.start_rate + progress * (segment.end_rate - segment.start_rate))

    return fallback_rate


def max_rate_for_profile(profile_name: str, fallback_rate: int) -> int:
    if profile_name == "bursty":
        return max(max(segment.start_rate, segment.end_rate) for segment in BURSTY_PROFILE)

    return fallback_rate
