# 数据库级严格两阶段锁与 SQL 事务

## API 与保证

新增可 Clone 的 TransactionManager::new_temp()，通过 begin() 返回 Transaction。
事务提供 run(SQL)、commit()、abort()，Drop 自动中止。所有 SQL 必须通过同一
manager 的事务进入；原 Database::run 仍为底层非事务 API，不自动获得隔离保证。
本阶段管理器为临时数据库，commit 仅保证进程内可见性，持久化在恢复阶段实现。

使用数据库级 S/X 锁：begin 获得 S，第一次 DDL/DML 执行前升级 X；S 可共享，
X 排他。锁一直持有到 commit/abort/Drop，符合严格 2PL。升级遇到其他读者或
begin 遇到写者立即返回 Transaction 错误（no-wait）；升级失败的事务自动中止
释放锁，避免升级死锁。没有解锁后继续申请锁的公共 API。

## 私有工作副本与发布

begin 在 manager Mutex 内 flush 当前提交版本，复制完整文件到独立临时数据库。
事务只读写自己的 BufferPool/Catalog/文件，读到自己的写入且不污染提交版本。
任何解析、计划或执行错误自动中止，丢弃工作副本，故部分写入也不会被发布。
commit 在持有 X 的情况下原子替换 manager 内的 Database；读事务只释放 S。
abort 及 Drop 丢弃工作副本。Mutex 保护锁表、事务号分配和版本替换，SQL 求值
在 Mutex 外执行，多个只读事务可以并行。事务 ID 用 checked_add 防止回绕。

完整文件复制开销为 O(数据库大小)，锁粒度粗导致不同表写入也冲突；这是可验证
的教育实现，不声称支持行锁、意向锁、跨进程共享或高吞吐量。后续 MVCC 将在
同一 API 中引入不阻塞写者的快照事务。

## 测试

DDL/DML commit、abort/Drop 回滚、读己之写、其他事务不可见、多个共享读者、
升级冲突自动回滚、写者排他、执行错误后无法提交、真实线程间读者/写者冲突，
以及提交后数据和索引读取。全部测试通过后独立提交推送。
