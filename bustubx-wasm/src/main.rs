use bustubx::{pretty_format_tuples, Database};
use std::io::{self, BufRead, Write};

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/data/database.db".into());
    let mut db = Database::new_on_disk(&path)?;
    let mut output = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match db.run(&line) {
            Ok(rows) => {
                db.flush()?;
                writeln!(output, "OK rows={}", rows.len())?;
                if !rows.is_empty() {
                    writeln!(output, "{}", pretty_format_tuples(&rows))?;
                }
            }
            Err(error) => writeln!(output, "ERROR {error}")?,
        }
        output.flush()?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR {error}");
        std::process::exit(1);
    }
}
