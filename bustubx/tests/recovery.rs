use bustubx::{IsolationLevel, TransactionManager};

#[test]
fn durable_commits_reopen_and_conflicts_do_not_publish() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("db.journal");
    {
        let manager = TransactionManager::new_on_disk(&path).unwrap();
        assert!(TransactionManager::new_on_disk(&path).is_err());
        let mut tx = manager.begin().unwrap();
        tx.run("create table t (a int)").unwrap();
        tx.run("insert into t values (1)").unwrap();
        tx.run("create index idx on t (a)").unwrap();
        tx.commit().unwrap();
        let mut a = manager
            .begin_with_isolation(IsolationLevel::SnapshotIsolation)
            .unwrap();
        let mut b = manager
            .begin_with_isolation(IsolationLevel::SnapshotIsolation)
            .unwrap();
        a.run("insert into t values (2)").unwrap();
        b.run("insert into t values (3)").unwrap();
        a.commit().unwrap();
        assert!(b.commit().is_err());
    }
    let manager = TransactionManager::new_on_disk(&path).unwrap();
    let mut tx = manager.begin().unwrap();
    let rows = tx.run("select a from t order by a").unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.data[0].to_string())
            .collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    tx.run("insert into t values (4)").unwrap();
    tx.commit().unwrap();
    drop(tx);
    drop(manager);
    assert_eq!(
        TransactionManager::new_on_disk(&path)
            .unwrap()
            .begin()
            .unwrap()
            .run("select a from t")
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("BUSTUBX_RECOVERY_CHILD_PATH") else {
        return;
    };
    let manager = TransactionManager::new_on_disk(path).unwrap();
    let mut tx = manager.begin().unwrap();
    tx.run("insert into t values (2)").unwrap();
    if std::env::var_os("BUSTUBX_RECOVERY_CHILD_COMMIT").is_some() {
        tx.commit().unwrap();
    }
    std::process::exit(0);
}

#[test]
fn process_exit_preserves_commits_and_discards_uncommitted_writes() {
    for committed in [false, true] {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("db.journal");
        {
            let manager = TransactionManager::new_on_disk(&path).unwrap();
            let mut tx = manager.begin().unwrap();
            tx.run("create table t (a int)").unwrap();
            tx.run("insert into t values (1)").unwrap();
            tx.commit().unwrap();
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "crash_child", "--nocapture"])
            .env("BUSTUBX_RECOVERY_CHILD_PATH", &path)
            .env_remove("BUSTUBX_RECOVERY_CHILD_COMMIT");
        if committed {
            child.env("BUSTUBX_RECOVERY_CHILD_COMMIT", "1");
        }
        assert!(child.status().unwrap().success());
        let manager = TransactionManager::new_on_disk(&path).unwrap();
        let rows = manager.begin().unwrap().run("select a from t").unwrap();
        assert_eq!(rows.len(), if committed { 2 } else { 1 });
    }
}
