# 事务快照的索引正确性依赖

2PL 回归在提交带索引数据库后重新 begin 时失败：CREATE INDEX 未回填已有行，
INSERT 引起的根页变化只在内存中更新，information_schema.indexes 仍保存 0。

创建索引先用 TableIterator 扫描表构建 B+ 树，再登记索引与系统目录。flush
之前扫描索引目录，按 schema/table/index 定位内存索引，将 root_page_id 同步
到对应元组；随后统一刷盘，保证导出快照和显式 flush 包含当前索引根。
空索引迭代直接 EOF，避免读取 INVALID_PAGE_ID。磁盘 read_page 对 0 返回错误，
避免 unsigned 下溢。此设计不提供崩溃原子性，后续事务持久化单独实现。

测试包括在已有记录上建索引、提交后重开、空索引、插入触发根分裂后重开。
事务测试和全工作区测试通过后独立提交推送，不重写已有用户提交。
