use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::CountryVecs;
use log::info;

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct CountryLine {

    pub iso: String,
    pub iso3: String,
    pub iso_numeric: i32,
    pub fip: Option<String>,
    pub country: String,
    pub capital: Option<String>,
    pub area_sqkm: f64,
    pub population: i64,
    pub continent: String,
    pub tld: Option<String>,
    pub currencycode: Option<String>,
    pub currencyname: Option<String>,
    pub phone: Option<String>,
    pub postalcodeformat: Option<String>,
    pub postalcoderegex: Option<String>,
    pub languages:  Option<String>,
    pub geonameid: i64,
    pub neighbours: Option<String>,
    pub equivalentfipscode: Option<String>,

}

#[derive(Debug)]
pub struct CountryRec {
    pub id: i64,
    pub rank: i32,
    pub iso_code: String,
    pub country_name: String,
    pub continent: String,
    pub tld: String,
    pub languages:  String,
    pub capital: String,
}


pub async fn import_countries_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(source_file_path)?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);

    let mut i = 0;
    let vector_size = 500;
    let mut dv: CountryVecs = CountryVecs::new(vector_size);
            
    for result in csv_rdr.deserialize() {
    
        let source: CountryLine = result?;

        let mut rank = 1;
        if source.population < 320000 {
            rank = 2;
        }

        let country_rec = CountryRec {
            id: source.geonameid,
            rank: rank,
            iso_code: source.iso,
            country_name: source.country.trim().replace(".", "").replace("'", "’"),
            continent: source.continent,
            tld: source.tld.unwrap_or("".to_string()),
            languages: source.languages.unwrap_or("".to_string()),
            capital: source.capital.unwrap_or("".to_string()).trim().replace(".", "").replace("'", "’"),
        };

        dv.add_data(&country_rec);   // transfer data to vectors
        i +=1;    

    }
            
    dv.store_data(&pool).await?;
    info!("{} records processed from {} to geo.countries", i, source_file_name);

    Ok(())
}

