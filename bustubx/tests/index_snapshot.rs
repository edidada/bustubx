use bustubx::Database;

#[test]
fn indexes_backfill_and_survive_root_changes_and_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("index.db");
    let mut db = Database::new_on_disk(path.to_str().unwrap()).unwrap();
    db.run("create table t (a int)").unwrap();
    db.run("insert into t values (1), (2)").unwrap();
    db.run("create index idx on t (a)").unwrap();
    assert_eq!(db.run("select a from t").unwrap().len(), 2);
    db.run("create table empty_table (b int)").unwrap();
    db.run("create index empty_idx on empty_table (b)").unwrap();
    assert!(db.run("select b from empty_table").unwrap().is_empty());
    for start in (3..503).step_by(50) {
        let rows = (start..start + 50)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        db.run(&format!("insert into t values {rows}")).unwrap();
    }
    db.flush().unwrap();
    drop(db);
    let mut db = Database::new_on_disk(path.to_str().unwrap()).unwrap();
    let rows = db.run("select a from t order by a").unwrap();
    assert_eq!(rows.len(), 502);
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.data[0].to_string(), (i + 1).to_string());
    }
    assert!(db.run("select b from empty_table").unwrap().is_empty());
}
