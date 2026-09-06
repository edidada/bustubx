# 全库版本快照 MVCC

## 模型与 API

新增 IsolationLevel::{Serializable, SnapshotIsolation} 与
TransactionManager::begin_with_isolation(level)。begin() 保持严格 2PL。
管理器保存单调 commit_version，事务保存 base_version 和私有完整数据库副本。
每个活跃事务持有独立版本的文件、Catalog 和 BufferPool，因此旧版本在新提交后
仍可读；事务退出时由 TempDir 清理旧版本，管理器只保留最新提交版本。

快照事务 begin 不持有 S 锁，也不因已有未提交写者而阻塞；它读取最新已提交
数据库。自身写入仅修改私有副本。只读提交不校验版本，始终可以结束；写提交
必须 base_version == commit_version，并成功获得数据库 X 锁，才能在 Mutex 内
发布副本并增加版本。否则中止并丢弃副本（first committer wins）。
严格 2PL 与快照共用发布锁；严格读者/写者会阻止快照写提交，避免破坏原隔离。
快照读者不阻塞任何写者。事务错误和 Drop 行为沿用 2PL。

## 范围

以整库作为版本和冲突单位，代价是 O(数据库大小) 复制及不同行写入也可能冲突；
没有元组版本链和行级垃圾回收，不宣称达到生产 MVCC 性能。数据库级保守冲突
检测避免写偏差和丢失更新，索引/目录与表数据属于同一快照，不读其他版本索引。
版本号 checked_add 防止回绕，事务私有句柄不对调用方暴露以防绕过校验。

## 验证

旧快照跨多个提交重复读取一致、读己之写、不见未提交数据、两个写者只一个提交、
冲突方不能再次提交、只读旧快照可结束、DDL/索引同版本、2PL/MVCC 混用、
真实线程通过 Barrier 同时写后只有一个提交。全工作区回归通过后独立提交推送。
