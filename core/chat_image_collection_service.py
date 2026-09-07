from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .message_images import MessageImage

CHARACTERS = (
    ("初音未来", "Hatsune Miku"), ("镜音铃", "Kagamine Rin"), ("镜音连", "Kagamine Len"), ("巡音流歌", "Megurine Luka"), ("MEIKO", "MEIKO"), ("KAITO", "KAITO"),
    ("星乃一歌", "Ichika Hoshino"), ("天马咲希", "Saki Tenma"), ("望月穗波", "Honami Mochizuki"), ("日野森志步", "Shiho Hinomori"), ("花里实乃理", "Minori Hanasato"), ("桐谷遥", "Haruka Kiritani"), ("桃井爱莉", "Airi Momoi"), ("日野森雫", "Shizuku Hinomori"),
    ("小豆泽心羽", "Kohane Azusawa"), ("白石杏", "An Shiraishi"), ("东云彰人", "Akito Shinonome"), ("青柳冬弥", "Toya Aoyagi"), ("天马司", "Tsukasa Tenma"), ("凤笑梦", "Emu Otori"), ("草薙宁宁", "Nene Kusanagi"), ("神代类", "Rui Kamishiro"), ("宵崎奏", "Kanade Yoisaki"), ("朝比奈真冬", "Mafuyu Asahina"), ("东云绘名", "Ena Shinonome"), ("晓山瑞希", "Mizuki Akiyama"),
)
GROUPS = (
    ("VIRTUAL SINGER", ("初音未来", "镜音铃", "镜音连", "巡音流歌", "MEIKO", "KAITO")), ("Leo/need", ("星乃一歌", "天马咲希", "望月穗波", "日野森志步")), ("MORE MORE JUMP!", ("花里实乃理", "桐谷遥", "桃井爱莉", "日野森雫")), ("Vivid BAD SQUAD", ("小豆泽心羽", "白石杏", "东云彰人", "青柳冬弥")), ("Wonderlands×Showtime", ("天马司", "凤笑梦", "草薙宁宁", "神代类")), ("25时、Nightcord见。", ("宵崎奏", "朝比奈真冬", "东云绘名", "晓山瑞希")),
)

JAPANESE_NAMES = (
    '初音ミク', '鏡音リン', '鏡音レン', '巡音ルカ', 'MEIKO', 'KAITO',
    '星乃一歌', '天馬咲希', '望月穂波', '日野森志歩',
    '花里みのり', '桐谷遥', '桃井愛莉', '日野森雫',
    '小豆沢こはね', '白石杏', '東雲彰人', '青柳冬弥',
    '天馬司', '鳳えむ', '草薙寧々', '神代類',
    '宵崎奏', '朝比奈まふゆ', '東雲絵名', '暁山瑞希',
)
KNOWN_PAIRINGS = {'杏豆': ('白石杏', '小豆泽心羽'), '遥实': ('桐谷遥', '花里实乃理')}

@dataclass
class ChatImageCollection:
    images: dict[str, MessageImage] = field(default_factory=dict)
    candidates: list[dict[str, Any]] = field(default_factory=list)
    saved: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

class ChatImageCollectionService:
    def __init__(self, db, importer, data_dir: Path, max_candidates: int = 26):
        self.db, self.importer, self.data_dir = db, importer, data_dir
        self.max_candidates = max_candidates
        self._candidate_lock = asyncio.Lock()
        self._candidates: list[dict[str, Any]] | None = None

    def _initialize_candidates(self):
        ids = {}
        result = []
        for (name, alias), japanese in zip(CHARACTERS, JAPANESE_NAMES):
            matches = [self.db.resolve_tag(value, allow_fuzzy=False) for value in (name, japanese, alias)]
            existing_matches = {match.tag_id: match for match in matches if match.matched}
            existing = existing_matches[min(existing_matches)] if existing_matches else None
            canonical = existing.tag_name if existing else name
            duplicates = [match.tag_name for match in existing_matches.values() if match.tag_name != canonical]
            if duplicates:
                self.db.merge_tags(canonical, duplicates)
            ids[name] = self.db.get_or_create_tag(canonical, tag_type='character')
            for value in (name, japanese, alias):
                if value != canonical:
                    self.db.add_alias(canonical, value)
            result.append({'tag_id': ids[name], 'name': canonical, 'standard_name': name,
                           'name_en': alias, 'name_ja': japanese, 'tag_type': 'character'})
        for name, members in GROUPS:
            result.append({'tag_id': self.db.get_or_create_tag(name, tag_type='theme'), 'name': name, 'tag_type': 'theme', 'member_ids': [ids[x] for x in members]})
        for alias in ('25时', '25時', '25時、ナイトコードで。'):
            self.db.add_alias('25时、Nightcord见。', alias)
        for name in KNOWN_PAIRINGS:
            row = self.db.get_tag_row(name)
            if row is not None and row['tag_type'] != 'pairing':
                self.db.set_tag_type(name, 'pairing')
        for row in self.db.list_tags(keyword='', limit=200, character_only=None):
            if str(row['status'] or 'active') == 'active' and str(row['tag_type'] or '') == 'pairing':
                candidate = {'tag_id': int(row['id']), 'name': str(row['name']), 'tag_type': 'pairing'}
                if row['name'] in KNOWN_PAIRINGS:
                    candidate['member_ids'] = [ids[x] for x in KNOWN_PAIRINGS[row['name']]]
                result.append(candidate)
        return result

    def candidate_tags(self):
        if self._candidates is None:
            raise RuntimeError('candidate tags are initialized by prepare')
        return [dict(item) for item in self._candidates]

    async def prepare(self, images: list[MessageImage]) -> ChatImageCollection:
        if self._candidates is None:
            async with self._candidate_lock:
                if self._candidates is None:
                    self._candidates = self._initialize_candidates()
        return ChatImageCollection({item.ref: item for item in images if item.location}, self.candidate_tags())

    async def save(self, state: ChatImageCollection, image_ref: str, tag_ids: list[int], reason: str = ''):
        async with state.lock:
            candidates = {int(x['tag_id']): x for x in state.candidates}
            item = state.images.get(str(image_ref))
            if item is None: return {'ok': False, 'message': '图片引用已失效，只能保存本次请求实际看到的图片。'}
            if not isinstance(tag_ids, list) or not tag_ids or any(type(x) is not int for x in tag_ids) or len(set(tag_ids)) != len(tag_ids) or not set(tag_ids) <= candidates.keys():
                return {'ok': False, 'message': '只能选择本次请求提供的唯一有效整数 tag ID。'}
            if not any(candidates[x]['tag_type'] == 'character' for x in tag_ids):
                return {'ok': False, 'message': '至少选择一个 PJSK 角色 tag。'}
            if any(not set(candidates[x].get('member_ids', [])).issubset(tag_ids) for x in tag_ids):
                return {'ok': False, 'message': '组合标签的成员未全部选中，请只标注实际出现的角色。'}
            try:
                imported = await item.import_into(self.importer)
                tags = [candidates[x] for x in tag_ids]
                result = self.db.commit_chat_collection_image(image_id=int(imported.image_id), image_url=item.location, author=str(item.metadata.get('source_sender_name', '')), raw_tags=[x['name'] for x in tags], extra_json={'source_kind': 'chat_auto_collection', **item.metadata}, tag_ids=tag_ids, reason=reason, tag_members={x: candidates[x].get('member_ids', []) for x in tag_ids})
            except Exception as exc:
                return {'ok': False, 'message': f'保存失败：{type(exc).__name__}'}
            accepted = result['tag_ids_accepted']
            result = {'ok': bool(accepted), 'image_id': int(imported.image_id),
                      'tags': [candidates[x]['name'] for x in accepted], **result}
            previous = state.saved.get(str(image_ref))
            cumulative = dict(result)
            if previous:
                cumulative['changed'] = previous['changed'] or result['changed']
                cumulative['tags'] = list(dict.fromkeys([*previous['tags'], *result['tags']]))
            state.saved[str(image_ref)] = cumulative
            return result

    async def summary(self, state: ChatImageCollection) -> str:
        async with state.lock: saved = list(state.saved.values())
        changed = [x for x in saved if x.get('changed')]
        names = list(dict.fromkeys(n for x in changed for n in x['tags']))
        return f"顺手收了 {len({x['image_id'] for x in changed})} 张：" + '、'.join(names) if changed else ''
