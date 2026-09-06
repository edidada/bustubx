use bustubx::{BustubxError, TransactionManager};

fn setup() -> TransactionManager {
    let manager = TransactionManager::new_temp().unwrap();
    let mut tx = manager.begin().unwrap();
    tx.run("create table t (a int)").unwrap();
    tx.run("insert into t values (1)").unwrap();
    tx.commit().unwrap();
    manager
}

#[test]
fn sql_commit_abort_drop_and_errors_are_atomic() {
    let manager = setup();
    let mut tx = manager.begin().unwrap();
    tx.run("insert into t values (2)").unwrap();
    assert_eq!(tx.run("select a from t").unwrap().len(), 2);
    tx.abort();
    assert!(tx.commit().is_err());
    assert_eq!(
        manager
            .begin()
            .unwrap()
            .run("select a from t")
            .unwrap()
            .len(),
        1
    );
    {
        let mut tx = manager.begin().unwrap();
        tx.run("create table discarded (a int)").unwrap();
        tx.run("insert into t values (3)").unwrap();
    }
    let mut tx = manager.begin().unwrap();
    assert_eq!(tx.run("select a from t").unwrap().len(), 1);
    assert!(tx.run("select * from discarded").is_err());
    assert!(tx.commit().is_err());
    let mut tx = manager.begin().unwrap();
    tx.run("insert into t values (4)").unwrap();
    assert!(tx.run("insert into t values ('bad')").is_err());
    assert!(tx.commit().is_err());
    let mut tx = manager.begin().unwrap();
    assert_eq!(tx.run("select a from t").unwrap().len(), 1);
    tx.run("create index idx on t (a)").unwrap();
    tx.run("insert into t values (5)").unwrap();
    tx.commit().unwrap();
    let rows = manager
        .begin()
        .unwrap()
        .run("select a from t order by a")
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.data[0].to_string())
            .collect::<Vec<_>>(),
        vec!["1", "5"]
    );
}

#[test]
fn strict_two_phase_locking_allows_readers_and_aborts_conflicting_upgrades() {
    let manager = setup();
    let mut first = manager.begin().unwrap();
    let mut second = manager.begin().unwrap();
    assert_ne!(first.id(), second.id());
    assert_eq!(
        first.run("select a from t").unwrap(),
        second.run("select a from t").unwrap()
    );
    assert!(matches!(
        first.run("insert into t values (2)"),
        Err(BustubxError::Transaction(_))
    ));
    assert!(first.commit().is_err());
    second.run("insert into t values (3)").unwrap();
    assert!(matches!(manager.begin(), Err(BustubxError::Transaction(_))));
    second.commit().unwrap();
    assert_eq!(
        manager
            .begin()
            .unwrap()
            .run("select a from t")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn locks_are_shared_across_real_threads() {
    let manager = setup();
    let reader = manager.begin().unwrap();
    let other = manager.clone();
    std::thread::spawn(move || {
        let mut tx = other.begin().unwrap();
        assert!(matches!(
            tx.run("insert into t values (2)"),
            Err(BustubxError::Transaction(_))
        ));
    })
    .join()
    .unwrap();
    drop(reader);
    let other = manager.clone();
    std::thread::spawn(move || {
        let mut tx = other.begin().unwrap();
        tx.run("insert into t values (3)").unwrap();
        tx.commit().unwrap();
    })
    .join()
    .unwrap();
    assert_eq!(
        manager
            .begin()
            .unwrap()
            .run("select a from t")
            .unwrap()
            .len(),
        2
    );
}
