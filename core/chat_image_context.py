from __future__ import annotations

import copy
import re
import mimetypes
from collections import OrderedDict

from astrbot.api.message_components import Image
from astrbot.core.agent.message import ImageURLPart, TextPart

from .message_images import MessageImage, direct_message_images

REF_PATTERN = re.compile(r'\[gallery_image:(g[0-9a-f]{16})\]')


class ChatImageContext:
    def __init__(self):
        self.sessions = {}

    def capture(self, event):
        images = direct_message_images(event)
        if not images:
            return
        records = self.sessions.setdefault(event.unified_msg_origin, OrderedDict())
        records[str(event.message_obj.message_id)] = images
        event.set_extra('pjsk_gallery_image_sources', images)
        while len(records) > 200:
            records.popitem(last=False)
        event.set_extra('pjsk_gallery_image_markers', ' '.join(
            f'[gallery_image:{item.ref}]' for item in images
        ))

    async def prepare(self, event, req, *, attach_originals):
        records = self.sessions.get(event.unified_msg_origin, {})
        known = {item.ref: item for batch in records.values() for item in batch}
        for item in direct_message_images(event):
            known.setdefault(item.ref, item)
        locations = {item.location: item for item in known.values()}
        for item in known.values():
            if item.metadata.get('resolved_path'):
                locations[item.metadata['resolved_path']] = item
        chosen = {}
        visible_locations = set()

        def include(location):
            item = locations.get(location)
            if item is None:
                # Old multimodal history may survive a restart; keep its unknown origin explicit.
                item = MessageImage(Image(url=location) if location.startswith(('http', 'data:')) else Image(file=location),
                    {'session_id': event.unified_msg_origin, 'source_message_id': '',
                     'source_sender_id': '', 'source_sender_name': '', 'source_origin': 'historical_unknown'})
            chosen[item.ref] = item
            visible_locations.add(location)
            visible_locations.add(item.location)
            return item.ref

        current_refs = [include(str(url)) for url in req.image_urls or []]
        if current_refs:
            req.prompt = (req.prompt or '') + '\n本次请求附图按顺序对应 image_ref：' + '、'.join(current_refs)

        req.contexts = copy.deepcopy(req.contexts or [])
        context_texts = [req.prompt or '']
        for message in req.contexts:
            if message.get('role') != 'user':
                continue
            content = message.get('content', '')
            if isinstance(content, str):
                context_texts.append(content)
                continue
            annotated = []
            for part in content:
                if part.get('type') == 'text':
                    context_texts.append(part.get('text', ''))
                elif part.get('type') == 'image_url':
                    value = part['image_url']
                    location = value['url'] if isinstance(value, dict) else value
                    ref = include(location)
                    annotated.append({'type': 'text', 'text': f'下面这张图片的 image_ref={ref}'})
                annotated.append(part)
            message['content'] = annotated
        extra_parts = list(req.extra_user_content_parts or [])
        req.extra_user_content_parts = []
        for part in extra_parts:
            if isinstance(part, TextPart):
                context_texts.append(part.text)
            elif isinstance(part, ImageURLPart):
                ref = include(part.image_url.url)
                part.image_url.id = ref
                req.extra_user_content_parts.append(TextPart(text=f'下面这张图片的 image_ref={ref}'))
            req.extra_user_content_parts.append(part)

        if attach_originals:
            refs = set(REF_PATTERN.findall('\n'.join(context_texts)))
            for ref, item in known.items():
                if ref not in refs or item.location in visible_locations:
                    continue
                # Add the known original beside its identifier in the same model request.
                location = item.location
                if not location.startswith(('http://', 'https://', 'data:')):
                    mime = mimetypes.guess_type(location)[0] or 'image/png'
                    location = f'data:{mime};base64,' + await item.image.convert_to_base64()
                req.extra_user_content_parts.extend([
                    TextPart(text=f'上下文图片原图，image_ref={ref}'),
                    ImageURLPart(image_url=ImageURLPart.ImageURL(url=location, id=ref)),
                ])
                chosen[ref] = item
                visible_locations.add(item.location)
        return list(chosen.values())
