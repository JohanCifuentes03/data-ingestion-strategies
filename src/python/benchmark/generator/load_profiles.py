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
    LoadSegment(60, 90, 2_000, 30_000),
    LoadSegment(90, 180, 30_000, 30_000),
    LoadSegment(180, 210, 30_000, 5_000),
    LoadSegment(210, 240, 5_000, 45_000),
    LoadSegment(240, 300, 10_000, 10_000),
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
