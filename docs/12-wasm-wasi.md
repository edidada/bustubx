# WASM / WASI SQL 运行器

## 目标和范围

新增 workspace 包 bustubx-wasm，编译到 wasm32-wasip1，复用 bustubx 核心。
通过 Node 22 的 node:wasi Preview 1 运行实际 .wasm 文件，在预打开的 /data
目录保存原始 Database 页文件。每行一个 SQL，stdin 输入，stdout 输出 OK 行数
及格式化表格；SQL 错误输出 ERROR 并继续下一行；输入/打开/flush 错误导致非零退出。
成功语句后 flush，以便第二次启动读取结果。这是非事务底层 API，不保证失败
DML 的回滚或突然中断时原始页文件的崩溃原子性。

这个目标不是浏览器 wasm32-unknown-unknown，不提供 JS 对象绑定或 IndexedDB。
WASI Preview 1 没有本实现需要的线程和文件锁能力；Database::set_parallelism
在 WASM 上只接受 1，持久化 TransactionManager 明确拒绝 WASM。2PL/MVCC
原生版本的行为保持不变。避免运行时走到不支持的线程创建函数。

## 工具和依赖

仅新增依赖本地 bustubx 的包，不增加 npm 包。Node runner 使用内置 fs、path、
wasi；预打开用户指定目录为 /data。参考 Node 官方 WASI API：
https://nodejs.org/api/wasi.html 。runner 默认读取 release .wasm，用户也可传路径。
Rust 目标需 `rustup target add wasm32-wasip1`。
comfy-table 在 WASM 目标关闭默认 tty 特性（crossterm 没有 WASI 终端实现），
原生目标保留默认特性。

## 验证

`cargo build -p bustubx-wasm --target wasm32-wasip1 --release`，然后实际运行
Node smoke test：CREATE/INSERT、建索引、过滤排序、连接、COUNT/AVG、错误后
下一条查询以及同一页文件重开。smoke test 使用独立临时目录并清理自身目录。
还运行完整原生工作区测试和格式检查，全部成功后独立提交并推送。

## 实际验证

Rust 1.98.0、wasm32-wasip1 release 构建成功，Node 22.14.0 实际执行 .wasm 的
smoke tests 通过，涵盖具体 COUNT/AVG 和连接值、索引及重开文件。
Windows 下载使用 Invoke-WebRequest；无代理从 Rust 镜像取得组件后与官方
manifest 的 SHA-256 一致，再由 rustup 安装。无须更改系统代理设置。
