# LEFT / RIGHT / FULL OUTER JOIN

## 问题

逻辑规划器目前把 LEFT、RIGHT 和 FULL OUTER JOIN 都错误地转换为 INNER JOIN，
物理嵌套循环连接也只接受 INNER/CROSS。查询因此会静默丢失未匹配行，且计划中的
可空列信息与实际连接类型不一致。

## 语义和执行状态

逻辑规划器保留 SQL 中的 JoinType，继续使用 `build_join_schema` 将可能由外连接
补出的列标记为 nullable。物理执行器支持现有五种 JoinType：INNER、CROSS、
LEFT、RIGHT、FULL。

嵌套循环仍以左行为外层，并保持当前的有界并行策略：固定一条左行，最多读取
1024 条右行，并行合并记录和计算 ON 条件。每条左行额外记录是否至少匹配一次；
LEFT/FULL 在其右侧扫描结束且没有匹配时，输出“左行 + NULL 右行”。

RIGHT/FULL 使用右侧扫描序号作为稳定身份，在所有左行扫描期间记录匹配过的右行
序号。左侧全部结束后再扫描一次右输入，按原顺序输出“NULL 左行 + 未匹配右行”。
序号而不是 Tuple 值能够正确区分内容完全相同的重复行。这个匹配集合的空间复杂度
为 O(右侧匹配行数)；候选行和结果队列仍受 1024 行批次限制。当前 Volcano 子算子
只提供重放接口，没有稳定 RID，因此这是不改变执行接口的最小状态方案。

输出 Tuple 始终使用逻辑连接的 nullable schema。真实一侧复制原值，缺失一侧按每列
DataType 生成对应的 NULL。ON 求值为 true 时匹配，false 或 NULL 时不匹配；非布尔
结果返回 Execution 错误。输入错误和表达式错误按已有批次顺序传播。

## 生命周期和顺序

`init` 清空当前左行、匹配位图、右序号、未匹配输出阶段和待输出队列，并初始化
左右输入，因此同一计划可重复执行，也可作为嵌套循环右子树反复重放。EOF 被记住，
后续 `next` 不重新读取输入。结果顺序定义为：每条左行的右侧匹配顺序，紧接该左行
可能产生的 NULL 右行；所有左行结束后输出未匹配右行。没有 ORDER BY 的 SQL 不依赖
此顺序，但稳定顺序便于测试和调试。

## 测试

- SQL logic tests 覆盖 LEFT/RIGHT/FULL 的匹配与两侧未匹配、空表和重复行；算子测试覆盖布尔 NULL ON。
- 串行和 2/4 线程比较完全相同的结果，右侧超过一个批次。
- 算子测试覆盖 init 重用、反复 EOF、nullable 输出 schema 和非布尔错误。
- 规划测试确认三种 outer JoinType 不再降级为 Inner。
- 验证命令为 `cargo fmt --all -- --check`、`cargo test --workspace`、
  `cargo build -p bustubx-wasm --target wasm32-wasip1 --release` 和 `git diff --check`。

本功能实现已有 SQL 解析器支持的 `ON` 连接条件。USING/NATURAL JOIN 仍由规划器明确
拒绝，不在本次范围内。
