# astrbot_plugin_pjsk_pic

一期 MVP：

1. 从本地图库扫描图片建立 SQLite 索引
2. 支持自然语言触发，例如“看看初音未来”“来张miku”
3. 支持 tag 别名管理
4. 支持重扫图库、查看统计、查看 tag 数量

## 默认图库目录

如果配置项 `library_root` 为空，默认使用：

`data/plugin_data/astrbot_plugin_pjsk_pic/library`

建议目录结构：

```text
library/
  初音未来/
    1.jpg
    2.png
  天马司/
    a.webp
```

扫描时默认取图库根目录下的**第一层子目录名**作为 tag。

## 管理命令

- `/pjsk图库 重扫`
- `/pjsk图库 统计`
- `/pjsk图库 查看 初音未来`
- `/pjsk图库 别名添加 初音未来 miku`
- `/pjsk图库 别名删除 初音未来 miku`
- `/pjsk图库 别名查看 初音未来`
