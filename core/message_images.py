from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Mapping

from astrbot.api.message_components import Image
from .models import CrawlCandidate


@dataclass
class MessageImage:
    image: Image
    metadata: dict = field(default_factory=dict)

    @property
    def location(self) -> str:
        return str(self.image.url or self.image.file or '')

    @property
    def ref(self) -> str:
        value = f"{self.metadata.get('session_id', '')}|{self.metadata.get('source_message_id', '')}|{self.metadata.get('node_path', '')}|{self.metadata.get('image_index', '')}|{self.location}"
        return 'g' + hashlib.sha256(value.encode()).hexdigest()[:16]

    async def import_into(self, importer):
        if self.location.startswith(('https://', 'http://')):
            return await importer.import_candidate(CrawlCandidate(
                platform='submission', post_url='', image_url=self.location,
            ))
        path = await self.image.convert_to_file_path()
        return await importer.import_local_file(Path(path), platform='submission')


@dataclass
class ImageCollection:
    items: list[MessageImage] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def original_chain(event):
    raw = event.message_obj.raw_message
    if isinstance(raw, Mapping) and isinstance(raw.get('message'), list):
        return raw['message']
    return event.get_messages()


def component_kind(component) -> str:
    if isinstance(component, dict):
        return str(component.get('type', '')).lower()
    return component.__class__.__name__.lower()


def message_metadata(event) -> dict:
    return {
        'session_id': event.unified_msg_origin,
        'source_message_id': str(event.message_obj.message_id),
        'source_sender_id': event.get_sender_id(),
        'source_sender_name': event.get_sender_name(),
    }


def direct_message_images(event) -> list[MessageImage]:
    result = []
    for component in original_chain(event):
        if component_kind(component) != 'image':
            continue
        data = component.get('data', {}) if isinstance(component, dict) else None
        image = Image(file=data.get('file', ''), url=data.get('url', '')) if data is not None else component
        result.append(MessageImage(image, {**message_metadata(event), 'image_index': len(result) + 1}))
    return result


async def collect_submission_images(event) -> ImageCollection:
    result = ImageCollection()
    visited: set[str] = set()
    seen_images: set[str] = set()

    async def action(name, **params):
        response = await event.bot.call_action(name, **params)
        return response.get('data', response) if isinstance(response, dict) else response

    async def walk(chain, metadata):
        for position, component in enumerate(chain or [], 1):
            kind = component_kind(component)
            data = component.get('data', {}) if isinstance(component, dict) else None
            if kind == 'image':
                image = Image(file=data.get('file', ''), url=data.get('url', '')) if data is not None else component
                item = MessageImage(image, {**metadata, 'image_index': position})
                if item.location and item.location not in seen_images:
                    seen_images.add(item.location)
                    result.items.append(item)
            elif kind == 'forward':
                forward_id = str(data['id'] if data is not None else component.id)
                if forward_id in visited:
                    continue
                visited.add(forward_id)
                try:
                    payload = await action('get_forward_msg', message_id=forward_id)
                    nodes = payload['messages']
                    for index, node in enumerate(nodes, 1):
                        node_data = node.get('data', node) if node.get('type') == 'node' else node
                        sender = node_data.get('sender', {})
                        await walk(node_data.get('content', node_data.get('message', [])), {
                            **metadata, 'forward_id': forward_id,
                            'node_path': f"{metadata.get('node_path', '')}/{index}",
                            'source_message_id': str(node_data.get('message_id', '')),
                            'source_sender_id': str(sender.get('user_id', node_data.get('uin', ''))),
                            'source_sender_name': str(sender.get('nickname', node_data.get('name', ''))),
                        })
                except Exception as exc:
                    result.errors.append(f'转发节点 {position} 获取失败（{type(exc).__name__}）')
            elif kind == 'node':
                await walk(data.get('content', []) if data is not None else component.content,
                           {**metadata, 'node_path': f"{metadata.get('node_path', '')}/{position}",
                            'source_sender_id': str(data.get('uin', data.get('user_id', ''))) if data is not None else str(component.uin),
                            'source_sender_name': str(data.get('name', data.get('nickname', ''))) if data is not None else str(component.name)})
            elif kind == 'nodes':
                await walk(component.nodes, metadata)

    chain = original_chain(event)
    direct = [c for c in chain if component_kind(c) in {'image', 'forward', 'node', 'nodes'}]
    if direct:
        await walk(direct, message_metadata(event))
    else:
        for reply in chain:
            if component_kind(reply) != 'reply':
                continue
            reply_id = str(reply['data']['id'] if isinstance(reply, dict) else reply.id)
            # Fetch the original quoted message, before other plugins flatten Forward nodes.
            try:
                if hasattr(event, 'bot'):
                    payload = await action('get_msg', message_id=int(reply_id))
                    sender = payload.get('sender', {})
                    await walk(payload['message'], {
                        **message_metadata(event), 'reply_message_id': reply_id,
                        'source_message_id': reply_id,
                        'source_sender_id': str(sender.get('user_id', '')),
                        'source_sender_name': str(sender.get('nickname', '')),
                    })
                else:
                    await walk(reply.chain, {**message_metadata(event), 'reply_message_id': reply_id})
            except Exception as exc:
                result.errors.append(f'引用消息获取失败（{type(exc).__name__}）')
            break
    return result
