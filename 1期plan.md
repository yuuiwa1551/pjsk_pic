# 第一期 MVP 设计稿

## 一期目标
先实现一个“本地图库发图插件”：

1. 用户发送自然语言，如“看看初音未来”
2. 插件识别请求的图片 tag
3. 从本地图库中随机选取一张对应图片发送
4. 支持 tag 别名
5. 支持基础索引管理和重扫

一期先不做：

- 远程平台爬虫
- 自动审核
- WebUI
- 复杂推荐算法

---

## 一期范围

### 用户侧能力

1. 规则触发
   - 看看xxx
   - 来张xxx
   - 发一张xxx
   - 来点xxx图片
2. LLM Tool 触发
   - 用户自然语言中未命中规则时，允许模型调用本地发图工具
3. 返回结果
   - 成功：发送图片
   - 未命中：提示没有找到对应 tag
   - 空图库：提示该 tag 暂无图片

### 管理侧能力

1. 扫描图片目录并写入索引
2. 添加/删除 tag 别名
3. 查询 tag 数量
4. 查询某 tag 下图片数
5. 重新扫描图库

---

## 目录结构建议

插件目录：

`data/plugins/astrbot_plugin_local_image_lib/`

建议结构：

```text
data/plugins/astrbot_plugin_local_image_lib/
  main.py
  metadata.yaml
  _conf_schema.json
  README.md
  core/
    db.py
    models.py
    indexer.py
    matcher.py
    sender.py
```

插件运行数据目录：

`data/plugin_data/astrbot_plugin_local_image_lib/`

建议结构：

```text
data/plugin_data/astrbot_plugin_local_image_lib/
  image_index.db
  images/
    imported/
  thumbs/              # 可选，后续 WebUI 再用
```

用户可配置的原始图库目录：

例如：

`E:/ImageLibrary`

或：

`C:/Users/Administrator/astrbot/data/local_images`

注意：
一期不强制按目录名作为最终 tag，只做“扫描目录并映射为初始 tag”的能力。

---

## 数据库设计

一期建议 SQLite。

### 表：images

- id
- file_path
- file_name
- sha256
- width
- height
- format
- is_active
- created_at
- updated_at

### 表：tags

- id
- name
- normalized_name
- created_at

### 表：tag_aliases

- id
- tag_id
- alias
- normalized_alias

### 表：image_tags

- id
- image_id
- tag_id
- source_type
- created_at

### 表：send_logs

- id
- session_id
- image_id
- matched_tag
- sent_at

---

## tag 匹配规则

### 标准化
对 tag 和用户输入都做 normalize：

1. 转小写
2. 去前后空格
3. 全角半角统一
4. 可选：去掉“图片”“图图”“老婆”“来一张”等噪声词

### 匹配顺序

1. 精确匹配主 tag
2. 精确匹配 alias
3. 模糊匹配 alias
4. 模糊匹配主 tag

如果多个候选命中：

1. 优先精确匹配
2. 再按图片数量多的 tag 排序
3. 必要时返回候选列表提示用户 уточ清

---

## 发图逻辑

### 基础流程

1. 从消息中提取查询词
2. 查询 tag/alias
3. 找到 tag 后查询可用图片
4. 排除最近发过的图片
5. 随机选取一张
6. 发送图片
7. 写发送日志

### 去重策略

一期只做简单去重：

1. 按 session 记录最近 N 次发送的 image_id
2. 随机时优先排除最近发送过的图片
3. 如果候选过少，再允许重复

建议默认：

- 每个会话最近排重数量：20

---

## 触发设计

### 规则触发
使用正则或命令式自然语句触发：

- 看看(.+)
- 来张(.+)
- 发一张(.+)
- 来点(.+)图片

优点：

- 快
- 稳
- 不依赖 LLM

### LLM Tool 触发
提供工具：

- send_local_image_by_tag(tag: string, count: number=1)

用途：

- 用户说“我想看看初音未来的图”
- 模型判断应调用工具

建议：

- 一期 count 先只允许 1
- 避免一次刷太多图

---

## 入库设计

### 扫描来源
一期先扫本地目录。

建议支持两种方式：

1. 按目录映射 tag
   - 如 `图库/初音未来/*.jpg`
   - 自动给该目录下文件绑定 tag“初音未来”
2. 手工命令补 tag
   - 后续补，不一定一期全做完

### 扫描流程

1. 遍历图库根目录
2. 读取图片文件
3. 计算 sha256
4. 记录图片信息
5. 根据所在目录绑定初始 tag

### 去重

一期先用 sha256 去重。

相同文件：

- 不重复入库
- 只补充缺失 tag 关系

---

## 插件配置建议

建议 `_conf_schema.json` 至少包含：

1. `library_root`
   - 图片根目录
2. `trigger_patterns`
   - 触发短语模板
3. `enable_llm_tool`
   - 是否开启 LLM Tool
4. `recent_dedupe_count`
   - 最近去重数量
5. `allow_fuzzy_match`
   - 是否允许模糊匹配
6. `admin_only_manage`
   - 管理命令是否仅管理员可用

---

## 管理命令建议

### 图库管理

1. 重扫图库
   - 例如：`/图库 重扫`
2. 查看统计
   - 例如：`/图库 统计`
3. 查看 tag 图片数
   - 例如：`/图库 查看 初音未来`

### 别名管理

1. 添加别名
   - 例如：`/图库 别名添加 初音未来 miku`
2. 删除别名
   - 例如：`/图库 别名删除 初音未来 miku`
3. 查看别名
   - 例如：`/图库 别名查看 初音未来`

---

## 异常处理

### 常见失败提示

1. 没识别到 tag
   - “没看懂你要看什么图”
2. tag 不存在
   - “图库里还没有这个 tag”
3. tag 存在但没图
   - “这个 tag 目前没有可发送图片”
4. 文件丢失
   - 自动跳过并记录日志

---

## 一期验收标准

满足以下条件即可认为 MVP 完成：

1. 能扫描本地目录入库
2. 能通过“看看xxx”成功发图
3. 能通过 alias 成功发图
4. 能避免短时间重复发同一张
5. 管理员能重扫图库和管理别名
6. 代码结构已为二期爬虫和审核预留扩展点

---

## 一期后续直接衔接的下一步

1. 增加图片级人工补 tag
2. 增加感知哈希去重
3. 增加批量导入器
4. 增加自动审核草稿流程
5. 再进入二期爬虫采集
