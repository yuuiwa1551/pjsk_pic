from dataclasses import dataclass, field


@dataclass
class MatchResult:
    matched: bool
    tag_id: int | None = None
    tag_name: str | None = None
    match_type: str | None = None
    candidates: list[str] = field(default_factory=list)
