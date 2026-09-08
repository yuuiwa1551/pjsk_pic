# 主聊天图片引用接入

图库在启用群中记录原消息图片引用，并在最终 LLM 请求中逐图标注 image_ref。上下文只有描述时，通过同会话引用取回原图，附入同一次请求。早于插件重启的历史多模态记录无法还原发送者时，标为未知，不冒用当前发言者。

当前 AstrBot 与 Group Chat Plus 的接入点：官方 group_chat_context.py 保留引用标记；Group Chat Plus 的普通、等待窗口、概率过滤缓存保留标记；图片下载记录原图与本地路径的对应关系。

在 AstrBot 容器中执行（第二个参数为独立备份目录）：

```text
python /AstrBot/data/plugins/astrbot_plugin_pjsk_pic/integrations/install_chat_image_markers.py /AstrBot /AstrBot/data/backups/pjsk-chat-markers
```

脚本备份三个原文件后修改接入点，需要重启 AstrBot。重建容器或升级 AstrBot/Group Chat Plus 后需重新应用。第三方源码不推送到作者仓库。

启用群配置保持原值。26 角色复用中日文名及 alias 对应的旧 ID；重复词条归并到最早的主 tag。组合标签只有成员全部选中时才可附加，普通同框不默认判 CP。收录不增加识图请求，不进入 shadow 审核，不覆盖人工拒绝。

按用户要求没有新增或运行测试，部署仅确认服务启动及版本/接入状态。

v0.23.2：用户随后授权专项测试。真实聊天确认了“中间正文先发送、随后执行工具”的顺序；收图状态现在由整轮 Agent 完成事件标记，最终回复才汇总和释放。本次运行生命周期 3 项与收图专项 6 项，均通过；不以这些模拟用例代替真实群聊入库记录。
