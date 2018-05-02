extern crate csv;
extern crate flate2;
#[macro_use]
extern crate serde_derive;
extern crate chrono;

use std::env;
use std::fs::File;
use std::io;

use chrono::{DateTime, Utc, Duration};
use flate2::read::GzDecoder;

#[derive(Debug, Deserialize)]
struct Row {
    bucket: String,
    key: String,
    size: usize,
    last_modified_date: DateTime<Utc>,
    etag: String,
}

fn main() {
    let mut scanned_rows_count = 0;
    let mut recent_rows_count = 0;
    let cutoff = Utc::now() - Duration::days(180);

    for filename in env::args().skip(1) {
        println!("{}", &filename);
        let (t, r) = count(&filename, cutoff).expect(&format!("Couldn't read {}", &filename));
        scanned_rows_count += t;
        recent_rows_count += r;
    }

    let percent = 100.0 * recent_rows_count as f32 / scanned_rows_count as f32;
    println!("{} / {} = {:.2}", recent_rows_count, scanned_rows_count, percent);
}

fn count(path: &str, cutoff: DateTime<Utc>) -> Result<(usize, usize), io::Error> {
    let mut input_file = File::open(&path)?;
    let decoder = GzDecoder::new(&mut input_file)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(decoder);

    let mut total = 0;
    let recent = reader.deserialize::<Row>()
        .flat_map(|row| row)  // Unwrap Somes, and skip Nones
        .inspect(|_| total += 1)
        .filter(|row| row.last_modified_date > cutoff)
        .count();

    Ok((total, recent))
}
