# 2026-09-09 历史图片导致收图钩子中断

- 任务：修复群聊自动收图没有注册工具及补入原图。
- 症状：真实 AstrBot 框架重放群聊历史，在 prepare 中抛出 Image.__init__() missing required argument: file。
- 原因：未知来源 URL/base64 分支使用 Image(url=location)，但框架要求显式 file；旧测试替身错误地给 file 默认值，未覆盖此分支。
- 修复：统一使用 Image(file=location)，URL、data URI 和本地路径交由框架 MediaResolver 处理。
- 预防：测试替身保持真实构造契约，验证旧历史图片和近期 marker 同时存在时仍能补图；不以独立服务测试替代框架接入验证。
- 证据：只读真实聊天历史包含 8 个 data URI 图片，未修改旧代码时 prepare 重放失败；不调用模型、不写正式图库。
- 修复后真实框架重放通过：8 次引用去重为 7 张；与近期 marker 混合时原图进入 assemble_context。首次验证错误地按引用次数断言图片数，改为按唯一 URL 比较。
