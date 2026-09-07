"""Apply the two small context marker integrations inside the AstrBot container.

Usage: python install_chat_image_markers.py /AstrBot /path/to/backup-directory
Only text/image metadata is changed; this does not run tests or invoke models.
"""
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
backup = Path(sys.argv[2])
backup.mkdir(parents=True, exist_ok=True)


def patch(path, label, replacements):
    text = path.read_text(encoding='utf-8')
    if '# pjsk-gallery-image-markers-v1' in text:
        return
    for old, new in replacements:
        if old not in text:
            raise RuntimeError(f'Integration source changed: {label}')
        text = text.replace(old, new, 1)
    shutil.copy2(path, backup / label)
    path.write_text(text, encoding='utf-8')
    print(f'Installed image references: {label}')


patch(root / 'astrbot/builtin_stars/astrbot/group_chat_context.py', 'group_chat_context.py', [(
    '        return "".join(parts)\n',
    '        # pjsk-gallery-image-markers-v1\n'
    '        markers = event.get_extra("pjsk_gallery_image_markers", "")\n'
    '        if markers:\n'
    '            parts.append(" " + markers)\n'
    '        return "".join(parts)\n',
)])

group_file = root / 'data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/main.py'
patch(root / 'data/plugins/plugin_upload_astrbot_plugin_group_chat_plus/utils/image_handler.py', 'group_image_handler.py', [
    ('ImageHandler._extract_image_urls(image_components)', 'ImageHandler._extract_image_urls(image_components, event)'),
    ('async def _extract_image_urls(image_components: List[Image])', 'async def _extract_image_urls(image_components: List[Image], event=None)'),
    ('                    image_urls.append(image_path)\n',
     '                    image_urls.append(image_path)\n'
     '                    # pjsk-gallery-image-markers-v1\n'
     '                    if event is not None:\n'
     '                        for source in event.get_extra("pjsk_gallery_image_sources", []):\n'
     '                            if source.location == str(img_component.url or img_component.file or ""):\n'
     '                                source.metadata["resolved_path"] = image_path\n'),
])
patch(group_file, 'group_chat_plus_main.py', [
    (
        '        cached_message = {\n            "role": "user",\n            "content": processed_message,',
        '        # pjsk-gallery-image-markers-v1\n'
        '        if image_retained and event.get_extra("pjsk_gallery_image_markers", ""):\n'
        '            processed_message += "\\n" + event.get_extra("pjsk_gallery_image_markers")\n'
        '        cached_message = {\n            "role": "user",\n            "content": processed_message,',
    ),
    (
        '                cached_message = {\n                    "role": "user",\n                    "content": processed_text,',
        '                if has_image and success and event.get_extra("pjsk_gallery_image_markers", ""):\n'
        '                    processed_text += "\\n" + event.get_extra("pjsk_gallery_image_markers")\n'
        '                cached_message = {\n                    "role": "user",\n                    "content": processed_text,',
    ),
    (
        '                    cached_message = {\n                        "role": "user",\n                        "content": processed_text,',
        '                    if has_image and success and event.get_extra("pjsk_gallery_image_markers", ""):\n'
        '                        processed_text += "\\n" + event.get_extra("pjsk_gallery_image_markers")\n'
        '                    cached_message = {\n                        "role": "user",\n                        "content": processed_text,',
    ),
])
