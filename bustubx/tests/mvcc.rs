use bustubx::{IsolationLevel, Transaction, TransactionManager};

fn snapshot(manager: &TransactionManager) -> Transaction {
    manager
        .begin_with_isolation(IsolationLevel::SnapshotIsolation)
        .unwrap()
}
fn setup() -> TransactionManager {
    let manager = TransactionManager::new_temp().unwrap();
    let mut tx = manager.begin().unwrap();
    tx.run("create table t (a int)").unwrap();
    tx.run("insert into t values (1)").unwrap();
    tx.commit().unwrap();
    manager
}

#[test]
fn old_snapshots_survive_commits_and_see_their_own_writes() {
    let manager = setup();
    let mut old = snapshot(&manager);
    let mut writer = snapshot(&manager);
    writer.run("insert into t values (2)").unwrap();
    assert_eq!(writer.run("select a from t").unwrap().len(), 2);
    assert_eq!(snapshot(&manager).run("select a from t").unwrap().len(), 1);
    writer.commit().unwrap();
    assert_eq!(old.run("select a from t").unwrap().len(), 1);
    assert_eq!(snapshot(&manager).run("select a from t").unwrap().len(), 2);
    let mut writer = snapshot(&manager);
    writer.run("update t set a = 7").unwrap();
    writer.commit().unwrap();
    assert_eq!(
        old.run("select a from t").unwrap()[0].data[0].to_string(),
        "1"
    );
    old.commit().unwrap();
    let rows = snapshot(&manager).run("select a from t").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.data[0].to_string() == "7"));
}

#[test]
fn first_committer_wins_and_mixed_locking_is_safe() {
    let manager = setup();
    let mut a = snapshot(&manager);
    let mut b = snapshot(&manager);
    a.run("insert into t values (2)").unwrap();
    b.run("insert into t values (3)").unwrap();
    a.commit().unwrap();
    assert!(b.commit().is_err());
    assert!(b.run("select a from t").is_err());
    let reader = manager.begin().unwrap();
    let mut writer = snapshot(&manager);
    writer.run("insert into t values (4)").unwrap();
    assert!(writer.commit().is_err());
    drop(reader);
    let mut strict = manager.begin().unwrap();
    strict.run("insert into t values (5)").unwrap();
    let mut old = snapshot(&manager);
    assert_eq!(old.run("select a from t").unwrap().len(), 2);
    strict.commit().unwrap();
    assert_eq!(old.run("select a from t").unwrap().len(), 2);
    old.commit().unwrap();
}

#[test]
fn concurrent_snapshot_writers_cannot_lose_committed_updates() {
    let manager = setup();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = (0..2)
        .map(|i| {
            let manager = manager.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut tx = snapshot(&manager);
                tx.run(&format!("insert into t values ({})", i + 2))
                    .unwrap();
                barrier.wait();
                tx.commit().is_ok()
            })
        })
        .collect::<Vec<_>>();
    let commits: usize = handles
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum();
    assert_eq!(commits, 1);
    assert_eq!(snapshot(&manager).run("select a from t").unwrap().len(), 2);
}

#[test]
fn schema_and_indexes_belong_to_the_same_snapshot() {
    let manager = setup();
    let mut old = snapshot(&manager);
    let mut writer = snapshot(&manager);
    writer.run("create index idx on t (a)").unwrap();
    writer.run("create table extra (b int)").unwrap();
    writer.run("insert into t values (2)").unwrap();
    writer.commit().unwrap();
    assert_eq!(old.run("select a from t").unwrap().len(), 1);
    assert!(old.run("select b from extra").is_err());
    let mut latest = snapshot(&manager);
    assert_eq!(latest.run("select a from t").unwrap().len(), 2);
    assert!(latest.run("select b from extra").unwrap().is_empty());
}
