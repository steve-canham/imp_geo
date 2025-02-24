use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::ScopeVecs;
use log::info;

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct ScopeLine {         
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
   
pub struct ScopeRec {
    pub id: i64,
    pub feature_code: String,
    pub name: String,
    pub members: Option<String>,
}

pub async fn import_scope_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(source_file_path)?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);
    
    let mut i = 0;
    let vector_size = 500;
    let mut dv: ScopeVecs = ScopeVecs::new(vector_size);

    let rgn = "RGN".to_string();
    let cont = "CONT".to_string();
            
    for result in csv_rdr.deserialize() {
    
        let source: ScopeLine = result?;

        let mut create_rec = false;
        match source.feature_code.clone() {
            Some(s) => {
                if s == rgn || s == cont {
                    create_rec = true;
                }
            },
            None => {},
        };

        if create_rec {
            let scope_rec = ScopeRec {
                id: source.geonameid,
                feature_code: source.feature_code.unwrap(),
                name: source.name,
                members: source.cc2,
            };

            dv.add_data(&scope_rec);   // transfer data to vectors
            i +=1;    

        }
    }
            
    dv.store_data(&pool).await?;
    info!("{} records processed from {} to geo.regions", i, source_file_name);

    Ok(())
}
