# 第十一期计划：pjsk 独立 WebUI 端口冲突修复

## 状态
- 当前状态：已完成
- 前置条件：第十期投稿审核命令增强已完成

## 本期目标

修复 `pjsk_pic` 独立 WebUI 与 `aiocqhttp` 反向 WS 端口冲突：

1. 将插件独立 WebUI 默认端口从 `6199` 调整到空闲的 `90xx`
2. 同时修正当前运行配置，避免重启后继续冲突
3. 验证 AstrBot 重启后不再出现 `Address already in use`

---

## 本期交付

- 默认 `webui_port` 改为 `9099`
- 当前本地配置中的 `webui_port` 同步改为 `9099`
- 重启 AstrBot 容器并确认冲突消失

---

## 涉及文件

- `data/plugins/astrbot_plugin_pjsk_pic/main.py`
- `data/plugins/astrbot_plugin_pjsk_pic/_conf_schema.json`
- `data/plugins/astrbot_plugin_pjsk_pic/README.md`
- `data/plugins/astrbot_plugin_pjsk_pic/metadata.yaml`
- `data/config/astrbot_plugin_pjsk_pic_config.json`
- `plan.md`

---

## 验证

- `py_compile` 语法检查
- 配置文件端口值检查
- AstrBot 容器重启后日志检查

---

## 结果

- `pjsk_pic` 独立 WebUI 默认端口已改为 `9099`
- 插件版本提升到 `v0.5.7`
