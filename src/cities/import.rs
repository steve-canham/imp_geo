use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::CityVecs;
use log::info;

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct CityLine {         
    pub geonameid: i64,
    pub name: String,
    pub asciiname: String,
    pub alternatenames: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub feature_class: Option<String>,
    pub feature_code: Option<String>,
    pub country_code: Option<String>,
    pub cc2: Option<String>,
    pub admin1_code: Option<String>, 
    pub admin2_code: Option<String>,
    pub admin3_code: Option<String>,
    pub admin4_code: Option<String>,
    pub population: Option<i64>,
    pub elevation: Option<i64>,
    pub dem: Option<i64>,
    pub timezone: Option<String>,
    pub mod_date: Option<String>,
}
   
pub struct CityRec {
    pub id: i64,
    pub name: String,
    pub disamb_type: String,
    pub disamb_code: String,
    pub country_code: String,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub population: Option<i64>,
}


pub async fn import_cities_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(source_file_path)?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);
    
    let mut i = 0;
    let vector_size = 10000;
    let mut dv: CityVecs = CityVecs::new(vector_size);

    let gb = "GB".to_string();
    let fr = "FR".to_string();
            
    for result in csv_rdr.deserialize() {
    
        let source: CityLine = result?;

        let mut disamb_type = "none".to_string();
        let mut disamb_code = "none".to_string();

        let country_code = match source.country_code {
           Some(s) => {
                disamb_type = "admin1".to_string();
                disamb_code = match source.admin1_code {
                    Some(s1) => s.clone() + "." + &s1,
                    None => "none".to_string(),
                };

                if s == gb || s == fr {
                disamb_type = "admin2".to_string();
                disamb_code = match source.admin2_code {
                    Some(s2) => disamb_code + "." + &s2,
                    None => "none".to_string(),
                    };
                }
                s
           },
           None => "none".to_string(),
        };
        

        let city_rec = CityRec {
            id: source.geonameid,
            name: source.name.replace("'", "’"),
            disamb_type: disamb_type,
            disamb_code: disamb_code,
            country_code: country_code,
            lat: source.latitude,
            lng: source.longitude,
            population: source.population,
        };


        dv.add_data(&city_rec);   // transfer data to vectors
        i +=1;    

    }
            
    dv.store_data(&pool).await?;
    info!("{} records processed from {} to geo.cities", i, source_file_name);

    Ok(())
}