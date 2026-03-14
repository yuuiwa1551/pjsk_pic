# astrbot_plugin_pjsk_pic

PJSK 图片图库插件。

当前已实现：

1. 第一期：本地图库随机发图
   - 自然语言触发：看看xxx / 来张xxx / 发一张xxx
   - tag / alias 检索
   - SQLite 索引
   - 本地图库重扫、统计、别名管理
   - LLM Tool 发图

2. 第二期：采集 + 自动审核
   - 采集任务队列
   - pixiv / X / 小红书 / lofter / generic 适配器骨架
   - 直链图片下载与公开页 meta 图片解析
   - 来源记录
   - 角色 tag 自动审核 / 人工审核命令

3. 第三期：WebUI 管理
   - 图片搜索与预览
   - tag / alias / 角色标记管理
   - 审核任务查看与处理
   - 采集任务查看与重试

## 命令

- `/pjsk图库 重扫`
- `/pjsk图库 统计`
- `/pjsk图库 查看 <tag>`
- `/pjsk图库 别名添加 <tag> <alias>`
- `/pjsk图库 别名删除 <tag> <alias>`
- `/pjsk图库 别名查看 <tag>`
- `/pjsk图库 角色标记 <tag> <true|false>`
- `/pjsk图库 采集添加 <platform> <url> [tags_csv]`
- `/pjsk图库 采集列表`
- `/pjsk图库 采集重试 <job_id>`
- `/pjsk图库 审核列表 [status]`
- `/pjsk图库 审核通过 <review_id>`
- `/pjsk图库 审核拒绝 <review_id>`

## WebUI

插件启动后会注册 WebUI：

- `/api/plug/pjsk_pic/ui`

可在 AstrBot Dashboard 登录后访问。
