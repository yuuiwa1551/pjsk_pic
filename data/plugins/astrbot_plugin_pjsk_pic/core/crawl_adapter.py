from __future__ import annotations

from .adapters import BaseCrawlAdapter, PixivAdapter, XAdapter, XiaohongshuAdapter


class GenericCrawlAdapter(BaseCrawlAdapter):
    def __init__(self, platform: str, config: dict | None = None) -> None:
        super().__init__(platform, config=config)


class CrawlAdapterFactory:
    SUPPORTED_PLATFORMS = {"pixiv", "x", "xiaohongshu", "lofter", "generic"}

    @classmethod
    def normalize_platform(cls, platform: str) -> str:
        value = (platform or "").strip().lower()
        aliases = {"twitter": "x", "xhs": "xiaohongshu", "rednote": "xiaohongshu"}
        return aliases.get(value, value)

    @classmethod
    def supports(cls, platform: str) -> bool:
        return cls.normalize_platform(platform) in cls.SUPPORTED_PLATFORMS

    @classmethod
    def create(cls, platform: str, config: dict | None = None) -> BaseCrawlAdapter:
        normalized = cls.normalize_platform(platform)
        if normalized == "pixiv":
            return PixivAdapter(config=config)
        if normalized == "x":
            return XAdapter(config=config)
        if normalized == "xiaohongshu":
            return XiaohongshuAdapter(config=config)
        if normalized in {"lofter", "generic"}:
            return GenericCrawlAdapter(normalized, config=config)
        raise ValueError(f"暂不支持的平台：{platform}")
