extern crate csv;
extern crate libflate;
#[macro_use]
extern crate serde_derive;
extern crate chrono;

use std::fs::File;
use std::io;
use std::env;

use chrono::{DateTime, Utc};
use libflate::gzip::Decoder;

#[derive(Debug, Deserialize)]
struct Row {
    bucket: String,
    key: String,
    size: usize,
    last_modified_date: DateTime<Utc>,
    etag: String,
}

fn main() {
    result_main().unwrap();
}

fn result_main() -> Result<(), io::Error> {
    let now = Utc::now();

    let mut scanned_rows_count = 0;
    let mut recent_rows_count = 0;

    for filename in env::args().skip(1) {
        let mut input_file = File::open(&filename)?;
        let decoder = Decoder::new(&mut input_file)?;
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(decoder);

        recent_rows_count += reader.deserialize::<Row>()
            .flat_map(|row| row)
            .inspect(|_| scanned_rows_count += 1)
            .filter(|row| (now - row.last_modified_date).num_days() <= 180)
            .count();
    }

    println!("scanned {} rows, {} were in the 180 days", scanned_rows_count, recent_rows_count);
    Ok(())
}
