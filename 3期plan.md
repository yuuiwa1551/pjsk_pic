# 第3期计划：WebUI 管理与检索

## 完成情况

- [x] 已完成并落地

## 目标
在一期、二期能力基础上，为图片库提供可视化管理页面，支持检索、审核、采集任务查看以及 tag 管理。

## 本期范围

1. WebUI 页面
   - 在 AstrBot Dashboard 中注册插件页面
   - 展示图库统计
   - 提供图片列表与图片预览

2. 图片检索
   - 按关键词 / tag / alias / 平台 / 审核状态搜索
   - 返回图片详情、标签状态、来源信息

3. tag 管理
   - 查看 tag 列表
   - 展示 alias
   - 设置 tag 是否为角色 tag
   - 增删 alias

4. 审核台
   - 查看审核任务
   - 手动通过 / 拒绝

5. 采集任务面板
   - 查看任务列表与状态
   - 新建采集任务
   - 失败任务重试

## Web API

1. 页面
   - `/api/plug/pjsk_pic/ui`

2. 数据接口
   - `/api/plug/pjsk_pic/api/summary`
   - `/api/plug/pjsk_pic/api/images`
   - `/api/plug/pjsk_pic/api/image`
   - `/api/plug/pjsk_pic/api/image-file`
   - `/api/plug/pjsk_pic/api/tags`
   - `/api/plug/pjsk_pic/api/jobs`
   - `/api/plug/pjsk_pic/api/jobs/retry`
   - `/api/plug/pjsk_pic/api/reviews`
   - `/api/plug/pjsk_pic/api/reviews/decision`
   - `/api/plug/pjsk_pic/api/tag/alias`
   - `/api/plug/pjsk_pic/api/tag/character`

## 完成标准

1. 能在 Dashboard 里打开插件管理页
2. 能检索图片并查看预览
3. 能查看并处理审核任务
4. 能查看并重试采集任务
5. 能管理 tag 别名和角色标记
