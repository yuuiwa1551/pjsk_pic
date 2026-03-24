# 第六期计划：独立 WebUI + 用户投稿 + 静默未命中

## 状态

- 当前状态：进行中
- 前置条件：第五期稳定性整改已完成

## 本期目标

围绕你当前提出的 3 个需求，完成一次偏架构型迭代：

1. 把当前 **Dashboard 内嵌 WebUI** 改成 **插件独立 WebUI**
2. 给图库新增 **用户投稿** 能力
3. 把自然语言发图中的 **未命中 tag 回复** 收敛为静默

---

## MVP 范围

### 1. 独立 WebUI

目标：不再把图库管理页面挂进 AstrBot Dashboard，降低耦合。

建议方案：

- 保留现有页面与 API 功能
- 改为插件启动时拉起独立 Web 服务
- 支持监听 `127.0.0.1` 或 `0.0.0.0`
- 默认监听 `0.0.0.0`
- 端口通过配置项指定
- 默认不再注册 Dashboard 内嵌页面

本期交付：

- WebUI 生命周期从 `context.register_web_api(...)` 中拆出
- 新增独立 WebUI 配置项，例如：
  - `webui_enabled`
  - `webui_host`
  - `webui_port`
  - `webui_access_token`（如需要）
- 新增一个管理员命令，用于查看当前 WebUI 访问地址

实现备注：

- 优先做“独立端口服务”
- 若后续仍有稳定性顾虑，再考虑升级为“独立进程”

---

### 2. 用户投稿

目标：允许普通用户通过消息 + 附图，把图片送进图库审核链路。

建议入口：

- `投稿 <角色tag>`
- `tg <角色tag>`

基础规则：

- 必须附带图片
- MVP 先限制为单条消息单图投稿
- tag 先按“角色 tag”场景设计
- 支持：
  - 文字在前、图片在后
  - 图片在前、文字在后

建议流程：

1. 从原始消息链中提取文本与图片组件
2. 将图片保存到插件本地目录
   - 建议沿用 `data/plugin_data/astrbot_plugin_pjsk_pic/images/imported/` 体系
3. 计算图片元数据、sha256、phash
4. 复用现有 DB 入库逻辑
5. 复用现有审核逻辑，对投稿 tag 跑一遍审核
6. 生成来源记录，标记为 `submission`
7. 根据审核结果：
   - 通过：进入可发图状态
   - 存疑 / 拒绝：进入审核任务队列

本期交付：

- 在 `main.py` 增加投稿命令与图片前置兜底触发
- 抽一个独立投稿服务模块，避免把图片提取/保存逻辑堆进 `main.py`
- 复用现有 `review_service.py`、`db.py`、`importer.py` 能力
- 在来源信息中记录：
  - 投稿人 ID / 昵称
  - 会话来源
  - 原始消息 ID（如可取）

MVP 暂不做：

- 多图批量投稿
- 投稿频率限制
- 黑白名单权限控制
- 单独的投稿历史管理页

---

### 3. 自然语言未命中静默

目标：减少误触发时的打扰感。

适用范围：

- `看看xx`
- `来张xx`
- `来一张xx`
- `发一张xx`
- `来点xx`

本期行为：

- 如果提取出的查询词没有匹配到任何 tag / alias
- 直接返回，不发送“图库里还没有这个 tag”之类提示

不改动：

- `/pjsk图库 ...` 管理命令
- LLM Tool 主动调用时的返回提示

---

## 技术决策

### A. WebUI 形态

本期推荐：

- **独立端口服务**
- 支持绑定 `127.0.0.1` 或 `0.0.0.0`
- 默认绑定 `0.0.0.0`
- 默认不再耦合 Dashboard

原因：

- 能最快满足“不要内嵌”的目标
- 改动范围可控
- 能最大化复用当前 `webui.py` 的 HTML + API 逻辑

未来可选增强：

- 独立进程模式
- 更完善的鉴权
- 反向代理 / Docker 暴露

### B. 投稿数据落地

本期推荐：

- 投稿图先走“导入图”路径
- 来源平台记为 `submission`
- 投稿人信息先放在来源 `extra_json`

原因：

- 尽量复用现有图片去重、审核、展示逻辑
- 避免第一版就引入过多新表与迁移

未来可选增强：

- 增加独立投稿记录表
- 单独投稿审核台 / 投稿历史页

### C. 静默未命中范围

本期推荐仅收敛：

- 自然语言触发未命中

不收敛：

- 显式命令
- 管理命令
- LLM Tool

原因：

- 用户这次明确提到的是“有人发看看xx”
- 保留后台管理与工具调用的可观测性

---

## 预估改动文件

核心会涉及：

- `data/plugins/astrbot_plugin_pjsk_pic/main.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/webui.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/importer.py`
- `data/plugins/astrbot_plugin_pjsk_pic/core/db.py`
- `data/plugins/astrbot_plugin_pjsk_pic/README.md`
- `data/plugins/astrbot_plugin_pjsk_pic/_conf_schema.json`
- `data/plugins/astrbot_plugin_pjsk_pic/metadata.yaml`

大概率新增：

- `data/plugins/astrbot_plugin_pjsk_pic/core/submission_service.py`

---

## 验证方案

### 1. 独立 WebUI

- 插件启动成功
- 独立端口可访问
- 原 Dashboard 内嵌入口不再注册
- 图片检索 / 审核 / 采集任务页面可正常工作

### 2. 用户投稿

- 单图投稿可成功识别
- 图片能落到本地目录
- 图片能进入数据库
- 审核任务能正确创建 / 复用
- 通过后图片可被正常发送

### 3. 静默未命中

- 命中 tag 时仍正常发图
- 未命中 tag 时不回复
- 管理命令与 LLM Tool 行为不受影响

---

## 延后项

以下内容建议不放进本期 MVP：

1. 投稿多图批处理
2. 投稿频率限制 / 权限控制
3. 投稿历史 WebUI 页面
4. 独立进程式 WebUI
5. 更复杂的 WebUI 登录鉴权

---

## 实施顺序建议

### Phase 6.1：独立 WebUI 拆分

先做 WebUI 拆分，因为这是最明显的架构变更。

交付重点：

- [x] 独立端口服务
- [x] Dashboard 解耦
- [x] 配置项补齐

### Phase 6.2：用户投稿

在独立 WebUI 稳定后，补消息入口。

交付重点：

- 投稿命令
- 图片提取
- 审核入库复用

### Phase 6.3：静默未命中 + 回归

最后收口交互与回归验证。

交付重点：

- 自然语言静默未命中
- 投稿 / 发图 / WebUI 联合回归
