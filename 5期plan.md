# 第五期 plan：pjsk_pic 稳定性整改

## 本期目标

针对 `astrbot_plugin_pjsk_pic` 做一轮高优先级稳定性整改，优先解决：

1. 同 sha256 图片在“本地图库 / 采集导入”双来源下的路径覆盖问题
2. 采集任务中单候选失败会拖垮整任务的问题
3. 本地图库扫描缺少错误日志的问题
4. Python 3.14 下 `imghdr` 不可用导致的兼容性问题

---

## 本期范围

### MVP 范围

- 为图片增加物理文件位置跟踪
- 保留逻辑图片去重能力，同时避免覆盖唯一可用路径
- 当当前路径失效时自动回退到其他仍可用路径
- 采集任务按候选图做容错，允许部分失败
- 增加扫描失败日志
- 移除 `imghdr` 依赖

### 本期不做

- WebUI 独立服务化
- WebUI 列表接口性能重构
- 角色 tag 判断策略重构
- `_conf_schema.json` 预留配置项清理
- 完整自动化测试体系建设

---

## 实施项

- [x] `core/db.py`
  - [x] 新增 `image_files` 物理路径表
  - [x] 为旧数据做路径表回填
  - [x] 调整 `upsert_image`
  - [x] 新增图片可用路径自动回退
- [x] `core/indexer.py`
  - [x] 写入 `library` 类型路径
  - [x] 增加扫描失败日志
- [x] `core/importer.py`
  - [x] 写入 `imported` 类型路径
  - [x] 移除 `imghdr`
- [x] `core/crawl_service.py`
  - [x] 单候选失败不中断整任务
  - [x] 将失败数量写入任务结果
- [x] `main.py` / `core/webui.py`
  - [x] 发图与图片文件接口改为解析可用路径

---

## 验证方式

- [x] `python -m compileall data/plugins/astrbot_plugin_pjsk_pic`
- [x] 路径回退 smoke test
  - 场景：同 sha256 图片同时存在 imported 与 library 路径，删除当前路径后仍能解析到备用路径
- [x] 采集部分失败 smoke test
  - 场景：两张候选图中一张失败、一张成功，任务应 `completed` 且 summary 含失败数量

---

## 本期产出

- 稳定性整改代码
- 更新后的总 `plan.md`
- 更新后的插件 `README.md`
- 更新后的 `metadata.yaml` 版本号

---

## 延后项

1. WebUI 从 AstrBot 内嵌改为独立服务
2. `api_images` 的 N+1 查询优化
3. 角色 tag 判断从启发式改为更保守策略
4. 清理未真正使用的配置项
5. 增加正式单元测试与集成测试
