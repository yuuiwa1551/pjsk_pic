# 第2期计划：采集链路 + 自动审核

## 完成情况

- [x] 已完成并落地

## 目标
在一期“本地图库发图”基础上，补齐图片采集、下载入库、自动审核、人工复核状态流转。

## 本期范围

1. 采集框架
   - 建立统一爬虫适配器接口
   - 提供 pixiv / X / 小红书 / lofter 适配器骨架
   - 支持公共页面解析、直链图片导入

2. 采集任务
   - 新建采集任务
   - 后台队列消费
   - 查看任务列表 / 状态 / 重试

3. 下载入库
   - 下载图片到插件数据目录
   - 以 sha256 去重
   - 记录来源信息、帖子链接、作者、原始 tag

4. 自动审核
   - tag 是否像角色名的启发式判断
   - 支持在 tag 级别手动标记 is_character
   - 调用多模态模型进行“图片是否符合角色 tag”判断
   - 审核状态分为 approved / pending / uncertain / rejected / manual_approved / manual_rejected

5. 人工复核
   - 查看审核任务
   - 手动通过
   - 手动拒绝

## 交付命令

1. 采集相关
   - `/pjsk图库 采集添加 <platform> <url> [tags_csv]`
   - `/pjsk图库 采集列表`
   - `/pjsk图库 采集重试 <job_id>`

2. 审核相关
   - `/pjsk图库 审核列表 [status]`
   - `/pjsk图库 审核通过 <review_id>`
   - `/pjsk图库 审核拒绝 <review_id>`

3. tag 管理增强
   - `/pjsk图库 角色标记 <tag> <true|false>`

## 数据层扩展

新增 / 扩展表：

1. `sources`
2. `crawl_jobs`
3. `review_tasks`
4. `tags.is_character`
5. `image_tags.review_status`
6. `image_tags.review_reason`
7. `image_tags.score`

## 完成标准

1. 能提交采集任务并后台执行
2. 能从直链或公开页面抓取到图片并入库
3. 能记录来源信息
4. 能对角色 tag 执行自动审核
5. 能通过命令查看并手动处理审核结果
6. 发图逻辑默认只使用审核通过图片
