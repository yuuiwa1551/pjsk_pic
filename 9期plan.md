# 第九期计划：投稿公开回执 + 管理员通知

## 状态
- 当前状态：已完成
- 前置条件：第八期投稿命令前缀收口已完成

## 本期目标

围绕投稿通知链路补一层“当前会话回执 + 后台管理员提醒”：

1. 投稿成功后的“已收录 / 等待审核”继续直接回复在当前对话里
2. 新投稿到达时，额外向 AstrBot 管理员或指定白名单发送提醒
3. 保持原有投稿解析、自动建 tag、补别名、审核入库流程不变

---

## MVP 范围

### 1. 当前会话公开回执

- 不改为私聊
- 不改为静默
- 保留当前投稿回执发送方式，继续在投稿发生的会话中公开回复

### 2. 管理员 / 白名单通知

- 新投稿成功后，额外发送一条“收到新投稿 #编号”通知
- 默认通知 AstrBot 配置中的 `admins_id`
- 支持附加白名单目标
- 白名单目标支持：
  - 直接填写用户 ID（按当前平台转成私聊会话）
  - 直接填写 `unified_msg_origin`

### 3. 通知内容

- 投稿编号（优先 review_id，兜底 image_id）
- 主 tag
- 本次新增别名
- 审核状态
- 投稿人
- 来源平台 / 会话 / 原消息
- 人工审核命令提示

---

## 配置项

- `submission_notify_enabled`
  - 是否启用投稿通知
- `submission_notify_use_astr_admins`
  - 是否默认通知 AstrBot 全局管理员
- `submission_notify_targets`
  - 额外通知目标，支持逗号/分号/换行分隔

---

## 涉及文件

- `data/plugins/astrbot_plugin_pjsk_pic/main.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/submission_service.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/submission_notify_service.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/__init__.py`
- `data/plugins/astrbot_plugin_pjsk_pic/_conf_schema.json`
- `data/plugins/astrbot_plugin_pjsk_pic/README.md`
- `data/plugins/astrbot_plugin_pjsk_pic/metadata.yaml`
- `plan.md`

---

## 验证

- `py_compile` 语法检查
- 投稿公开回执链路回归
- 通知目标解析 smoke test
- 通知文本构建 smoke test

---

## 结果

- 投稿成功后，当前对话仍会收到公开回执
- 新投稿会额外通知 AstrBot 管理员或指定白名单
- 插件版本提升到 `v0.5.5`
