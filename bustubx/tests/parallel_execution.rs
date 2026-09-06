use bustubx::Database;

#[test]
fn parallel_join_handles_large_right_side_empty_sides_and_no_matches() {
    let mut db = Database::new_temp().unwrap();
    db.run("create table l (a int)").unwrap();
    db.run("create table r (b int)").unwrap();
    db.run("create table empty_table (c int)").unwrap();
    db.run("insert into l values (1100), (1100), (1200)")
        .unwrap();
    let rows = (0..1300)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    db.run(&format!("insert into r values {rows}")).unwrap();
    for sql in [
        "select a, b from l inner join r on a = b",
        "select a, b from l inner join r on a < b limit 12 offset 5",
        "select a, b from l cross join r",
        "select a, b from l inner join r on a < 0",
        "select a, c from l cross join empty_table",
        "select c, b from empty_table cross join r",
    ] {
        db.set_parallelism(1).unwrap();
        let expected = db.run(sql).unwrap();
        for workers in [2, 4] {
            db.set_parallelism(workers).unwrap();
            assert_eq!(db.run(sql).unwrap(), expected, "{sql}");
        }
    }
}

#[test]
fn parallel_scan_covers_multiple_page_batches_and_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("scan.db");
    let mut db = Database::new_on_disk(path.to_str().unwrap()).unwrap();
    db.run("create table pages (a int)").unwrap();
    for start in (0..5000).step_by(100) {
        let rows = (start..start + 100)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        db.run(&format!("insert into pages values {rows}")).unwrap();
    }
    let expected = db.run("select a from pages").unwrap();
    assert_eq!(expected.len(), 5000);
    for (i, row) in expected.iter().enumerate() {
        assert_eq!(row.data[0].to_string(), i.to_string());
    }
    db.flush().unwrap();
    drop(db);
    let mut db = Database::new_on_disk(path.to_str().unwrap()).unwrap();
    for workers in [2, 4] {
        db.set_parallelism(workers).unwrap();
        assert_eq!(db.run("select a from pages").unwrap(), expected);
        let rows = db
            .run("select a from pages where a >= 4900 order by a desc limit 3 offset 2")
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.data[0].to_string())
                .collect::<Vec<_>>(),
            vec!["4997", "4996", "4995"]
        );
    }
}

#[test]
fn parallel_sql_matches_serial_across_batches_and_operators() {
    let mut db = Database::new_temp().unwrap();
    db.run("create table l (a int)").unwrap();
    db.run("create table r (b int)").unwrap();
    let left = (0..40)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    let right = (0..40)
        .map(|i| format!("({})", i % 7))
        .collect::<Vec<_>>()
        .join(",");
    db.run(&format!("insert into l values {left}")).unwrap();
    db.run(&format!("insert into r values {right}")).unwrap();

    for sql in [
        "select a > b from l, r",
        "select a, b from l, r where a >= 30",
        "select a, b from l, r where a > 50",
        "select a, b from l, r where a >= 30 limit 17 offset 3",
        "select a, b from l, r where a >= 30 order by a desc, b",
        "select count(a) from l, r where a >= 30",
        "select a, b from l, (select b from r where b >= 5)",
        "select a, b from l, (select b from r)",
        "select a from l where a > 12 order by a desc limit 9 offset 3",
        "select count(a) from l, r",
        "select b, count(b) from r group by b order by b",
        "select a from l where a < 0",
        "select a from l limit 0",
        "select a from l limit 3",
    ] {
        db.set_parallelism(1).unwrap();
        let expected = db.run(sql).unwrap();
        if sql == "select a > b from l, r" {
            assert_eq!(expected.len(), 1600);
        }
        for workers in [2, 4] {
            db.set_parallelism(workers).unwrap();
            assert_eq!(db.run(sql).unwrap(), expected, "{sql}, workers={workers}");
        }
    }
    db.run("insert into r values (NULL), (NULL)").unwrap();
    db.set_parallelism(1).unwrap();
    let expected = db.run("select b from r").unwrap();
    db.set_parallelism(4).unwrap();
    assert_eq!(db.run("select b from r").unwrap(), expected);

    db.run("create table copied (a int)").unwrap();
    db.run("insert into copied select a from l").unwrap();
    assert_eq!(db.run("select a from copied").unwrap().len(), 40);
}

#[test]
fn invalid_parallelism_is_rejected_and_database_remains_usable() {
    let mut db = Database::new_temp().unwrap();
    for workers in [0, 65, usize::MAX] {
        assert!(db.set_parallelism(workers).is_err());
    }
    for workers in [1, 4, 64] {
        db.set_parallelism(workers).unwrap();
        assert_eq!(db.run("select 7").unwrap().len(), 1);
    }
}
