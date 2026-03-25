# 第十期计划：投稿审核命令增强

## 状态
- 当前状态：已完成
- 前置条件：第九期投稿公开回执与管理员通知已完成

## 本期目标

补齐投稿审核的命令可操作性：

1. 管理员可直接通过命令查看当前待处理审核图片
2. 管理员可继续通过命令完成审核通过 / 拒绝
3. 管理员可通过命令开关“投稿审核”
4. 关闭投稿审核后，新投稿默认直接入库并参与发图

---

## 本期交付

### 1. 审核查看命令

- 新增 `/pjsk图库 审核查看 [review_id]`
- 不带 `review_id` 时，默认展示当前最新待处理审核图片
- 展示内容包含：
  - 图片本体
  - 审核任务编号
  - 状态
  - tag
  - 来源
  - 审核命令提示

### 2. 审核列表优化

- `/pjsk图库 审核列表` 默认查看待处理任务
- 继续保留按状态筛选的能力

### 3. 投稿审核开关

- 新增：
  - `/pjsk图库 投稿审核状态`
  - `/pjsk图库 投稿审核开启`
  - `/pjsk图库 投稿审核关闭`
- 开关结果持久化到插件配置

### 4. 投稿链路行为调整

- 当 `submission_review_enabled=false` 时：
  - 不再创建审核任务
  - 投稿图片直接按已通过状态入库
  - 可立即参与发图

---

## 涉及文件

- `data/plugins/astrbot_plugin_pjsk_pic/main.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/submission_service.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/db.py`
- `data/plugins/astrbot_plugin_pjsk_pic/_conf_schema.json`
- `data/plugins/astrbot_plugin_pjsk_pic/README.md`
- `data/plugins/astrbot_plugin_pjsk_pic/metadata.yaml`
- `plan.md`

---

## 验证

- `py_compile` 语法检查
- 审核查看命令选取待处理任务 smoke test
- 投稿审核开关持久化 smoke test
- 关闭投稿审核后的直接入库 smoke test

---

## 结果

- 管理员已可通过命令直接查看待处理审核图片
- 管理员已可通过命令开关投稿审核
- 插件版本提升到 `v0.5.6`
