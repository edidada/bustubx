# BustubX - a relational database for educational purpose (CMU 15-445)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/bustubx.svg)](https://crates.io/crates/bustubx)

- [x] Planner
- [x] Expression
- [x] Functions
- [x] Rule-based Optimizer
- [x] Volcano Executor
- [x] Disk Management
- [x] Buffer Pool
- [x] Table Heap
- [x] System Metadata (information_schema)
- [x] B+ Tree Index
- [x] Parallel Execution (supported read operators; page I/O and index scans remain serial)
  - [x] [Ordered, bounded parallel projection](docs/01-parallel-projection.md)
  - [x] [Demand-driven LIMIT and early termination](docs/02-demand-driven-limit.md)
  - [x] [Ordered, bounded parallel filtering](docs/03-parallel-filter.md)
  - [x] [Parallel table scan decoding](docs/05-parallel-table-scan.md)
  - [x] [Parallel INNER/CROSS joins](docs/06-parallel-nested-loop-join.md)
  - [x] [Parallel COUNT/AVG aggregation](docs/07-parallel-aggregation.md)
- [x] [Two Phase Locking](docs/08-strict-two-phase-locking.md) (database-level S/X, no-wait)
- [x] [Multi-Version Concurrency Control](docs/10-mvcc-snapshot-isolation.md) (whole-database snapshots, conservative write conflicts)
- [x] [Crash Recovery](docs/11-crash-recovery.md) (checksummed full-image commit journal)
- [ ] WASM

P.S. See [here](tests/sqllogictest/slt) to know which sql statements are supported already.

## Architecture
![architecture](./docs/bustubx-architecture.png)


## Get started
Use `TransactionManager::new_temp()?` and `manager.begin()?` for strict 2PL SQL
transactions. Call `tx.run(sql)?`, then `tx.commit()?`; `tx.abort()` or dropping the
transaction discards its writes. Conflicts and SQL errors abort the transaction.
This temporary manager provides process-local commits; direct `Database::run` is
the nontransactional API. See the [transaction design](docs/08-strict-two-phase-locking.md).
Use `manager.begin_with_isolation(IsolationLevel::SnapshotIsolation)?` for MVCC:
readers keep their original committed snapshot while writers commit new versions.
Use `TransactionManager::new_on_disk("database.journal")?` for durable commits and
automatic recovery after process crashes. Its journal format differs from raw
`Database` page files; see the [recovery design](docs/11-crash-recovery.md).

Read queries can opt into parallel projection, filtering, table scan decoding, INNER/CROSS
joins and COUNT/AVG aggregation using `db.set_parallelism(4)?` (1–64
workers; default: 1). Results preserve input order. Page I/O, index scans and writes remain serial;
small queries may be faster with the default. See the [projection design](docs/01-parallel-projection.md)
and [filter design](docs/03-parallel-filter.md)
for batching, error handling and current limitations.

Install rust toolchain first.
```
RUST_LOG=info,bustubx=debug cargo run --bin bustubx-cli
```

![demo](./docs/bustubx-demo.png)

## Reference
- [CMU 15-445/645 Database Systems](https://15445.courses.cs.cmu.edu/fall2022/)
- [cmu-db/bustub](https://github.com/cmu-db/bustub)
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs) and [blogs](https://frankma.me/categories/sqlrs/)
- [KipData/KipSQL](https://github.com/KipData/KipSQL)
- [talent-plan/tinysql](https://github.com/talent-plan/tinysql)
- [arrow-datafusion](https://github.com/apache/arrow-datafusion)
- [CMU 15-445课程笔记-zhenghe](https://zhenghe.gitbook.io/open-courses/cmu-15-445-645-database-systems/relational-data-model)
- [CMU15-445 22Fall通关记录 - 知乎](https://www.zhihu.com/column/c_1605901992903004160)
- [B+ Tree Visualization](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)
