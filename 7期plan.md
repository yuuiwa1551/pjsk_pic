# 第七期计划：投稿自动建 tag + 投稿补别名

## 状态
- 当前状态：已完成
- 前置条件：第六期独立 WebUI / 投稿 / 静默未命中已完成

## 本期目标

围绕投稿链路再做一轮可用性优化：

1. 投稿命中不存在的主 tag 时，自动创建主 tag
2. 允许用户在投稿时顺手补充一组别名
3. 增加别名冲突检测，避免和现有 tag / alias 打架

---

## MVP 范围

### 1. 投稿自动建 tag
- 保留 `投稿 <主tag>` / `tg <主tag>` + 附图 的入口
- 如果主 tag 不存在，则自动创建
- 自动创建出来的 tag 默认按角色 tag 处理
- 回执中明确提示“主 tag 已自动创建”

### 2. 投稿补别名
- 支持示例：
  - `投稿 晓山瑞希 别名:瑞希,mzk,mizuki,糖`
  - `tg 晓山瑞希 alias 瑞希,mzk,mizuki,糖`
- 多个别名建议使用逗号分隔
- 别名和主 tag 相同、重复提交的别名会自动跳过
- 成功补充的别名会在回执中展示

### 3. 冲突拦截
- 如果别名已经被当前主 tag 使用，则提示已存在
- 如果别名已经被其他主 tag 占用，则拒绝并提示冲突
- 如果别名和现有主 tag 名称冲突，则拒绝并提示冲突

---

## 涉及文件
- `data/plugins/astrbot_plugin_pjsk_pic/core/submission_service.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/db.py`
- `data/plugins/astrbot_plugin_pjsk_pic/main.py`
- `data/plugins/astrbot_plugin_pjsk_pic/README.md`
- `data/plugins/astrbot_plugin_pjsk_pic/metadata.yaml`
- `plan.md`

---

## 验证
- `py_compile` 语法检查
- 投稿文本解析 smoke test
- 投稿自动建 tag smoke test
- 投稿补别名 smoke test
- 别名冲突拦截 smoke test

---

## 结果
- 投稿命中不存在 tag 时可自动建 tag
- 投稿时可顺带补别名
- 别名冲突会被明确拦截
- 插件版本提升到 `v0.5.3`
