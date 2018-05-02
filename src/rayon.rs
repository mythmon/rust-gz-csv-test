extern crate csv;
extern crate flate2;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate rayon;

use std::env;
use std::fs::File;
use std::io;
use std::iter::Sum;

use chrono::{DateTime, Utc, Duration};
use flate2::read::GzDecoder;
use rayon::prelude::*;

#[derive(Debug, Deserialize)]
struct Row {
    bucket: String,
    key: String,
    size: usize,
    last_modified_date: DateTime<Utc>,
    etag: String,
}

struct Stats {
    total: usize,
    recent: usize,
}

impl Sum for Stats {
    fn sum<I: Iterator<Item=Self>>(iter: I) -> Self {
        let mut acc = Stats { total: 0, recent: 0 };
        for stat in  iter {
            acc.total += stat.total;
            acc.recent += stat.recent;
        }
        acc
    }
}

fn main() {
    let cutoff = Utc::now() - Duration::days(180);
    let filenames: Vec<String> = env::args().skip(1).collect();

    let stats: Stats = filenames.par_iter()
        .map(|filename| count(&filename, cutoff).expect(&format!("Couldn't read {}", &filename)))
        .sum();

    let percent = 100.0 * stats.recent as f32 / stats.total as f32;
    println!("{} / {} = {:.2}%", stats.recent, stats.total, percent);
}

fn count(path: &str, cutoff: DateTime<Utc>) -> Result<Stats, io::Error> {
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

    Ok(Stats { total, recent })
}
