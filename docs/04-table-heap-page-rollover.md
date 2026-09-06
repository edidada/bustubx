# 多页表写入正确性

## 问题与设计

TableHeap::insert_tuple 分配下一页后只更新 page_id 和 TablePage 副本，却未
更新持有的 PageRef。最后写回的是旧页，破坏旧数据及 next_page_id 链接。
将 PageRef 设为可变，在完成旧页链接写回后切换为新页引用，释放旧页 pin。
这样最终数据写入、返回的 RecordId 和 last_page_id 始终指向同一页。
本次不宣称支持并发写入；Database 的 SQL 调用仍串行。

## 验证

使用 2 帧缓冲池插入 500 条 Int32 数据，强制多页与驱逐。逐 RID 读取检查
原值；顺序迭代检查完整性、顺序与 EOF；flush 后重建缓冲池和表，再验证全部
记录。先在旧代码运行回归确认失败，再修复并跑全工作区测试。通过后独立提交。
