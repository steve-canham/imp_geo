use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
//use super::data_vectors::AltRecVecs;
//use log::info;


pub async fn import_countries_data(data_folder: &PathBuf, source_file_name: &str, _pool: &Pool<Postgres>, _latin_only: bool) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(source_file_path)?;
    let buf_reader = BufReader::new(file);
    let _csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);
    Ok(())
}