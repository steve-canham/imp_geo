use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
//use super::data_vectors::AltRecVecs;
//use log::info;

/* 
#[derive(serde::Deserialize)]
struct LangCodeLine {
    #[serde(rename = "ISO 639-3")]
    pub c3: String,
    #[serde(rename = "ISO 639-2")]
    pub c2: String,
    #[serde(rename = "ISO 639-1")]
    pub c1: String,
    #[serde(rename = "Language Name")]
    pub name: String,
}


#[derive(Debug)]
pub struct LangCodeRec {
    pub c3: String,
    pub c2: String,
    pub c1: String,
    pub name: String,
}
*/

pub async fn import_cities_data(data_folder: &PathBuf, source_file_name: &str, _pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(source_file_path)?;
    let buf_reader = BufReader::new(file);
    let _csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);
    Ok(())
}