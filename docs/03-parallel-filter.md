# 并行执行第二阶段：有界批量过滤

## 目标

将 `Database::set_parallelism` 扩展到 WHERE 谓词求值。数据库默认仍为串行，
写计划（包括 INSERT SELECT）仍整体使用串行执行。保持输入顺序与现有谓词
求值语义，不顺带实现尚缺失的 SQL 三值逻辑、算术表达式或并行存储扫描。

## 执行流程

PhysicalFilter 增加 parallelism、Mutex 状态（待输出队列与 EOF 标志），由
PhysicalPlanner 传入并行度。串行分支持续拉取一行并判断谓词；并行分支由
调用线程拉取最多 1024 行，只把不可变 Tuple 与 Expr 传给 ordered_map。
线程按连续分片计算 `Result<bool>`，所有线程 join 后按原始行序合并：

- true：将原 Tuple 移入输出队列，不克隆整行；
- false：丢弃该行；
- Err：将错误保存在对应的位置，等 next 消费到它时才返回。

如果整个批次没有输出，next 必须继续下一批，不能误报 EOF。子输入错误排在
该批已有输出之后并结束预取。输入 EOF 也保存在状态中，重复 next 不再读子节点。
init 清空队列和结束状态并初始化子节点，支持嵌套循环连接重复扫描。

## 公共并行工具与资源上限

沿用 scoped threads 和 ordered_map，不增加依赖。将公共线程名称和故障信息
从 projection 改为 execution，便于多个算子复用；将批大小提到 parallel 模块，
投影和过滤共享 1024 行上限。各算子收集输入时未启动自己的工作线程，故嵌套
算子不会叠加工作线程数量。每个算子缓存至多一批，整棵计划的缓存量随算子数增长。
普通错误按消费顺序返回；线程启动失败或 panic 仍可提前报告。

当前过滤器对于 Boolean(None) 返回错误，本次保持该行为以便串并行对照；
SQL 三值逻辑应在独立设计和测试中修复。小查询可能因线程开销更慢，不承诺提速。

## 测试计划

- SQL：1/2/4 线程结果一致；1600 行连接输入、多批次、全拒绝批后仍有匹配、
  全拒绝、重复值、LIMIT/OFFSET、ORDER BY、聚合、带过滤的子查询连接重初始化。
- 算子：true/false 与现有 NULL/非布尔错误语义一致，普通错误延迟到消费时，
  输入错误保持顺序，LIMIT 已满足时不抛出后续错误，init 清理未消费队列。
- 计划：读计划包含 ParallelFilter，写计划整棵子树不含并行算子。
- `cargo test --workspace`（包括串并行 SQL logic test）、fmt、diff 检查通过
  后，单独 commit 和 push；README 更新当前进度与 API 范围。

## 验证结果

`cargo test --workspace` 通过：52 个库单测、2 个 SQL 集成测试，以及分别在
1/4 线程执行的完整 SQL logic test 套件。1600 行连接的 WHERE a >= 30
在首个 1024 行批次全部被拒绝后仍正确输出后续 400 行；结果与串行一致。
计划测试验证 SELECT 同时使用并行投影和过滤，INSERT SELECT 的子树仍串行。
