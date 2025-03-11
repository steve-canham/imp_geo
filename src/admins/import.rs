use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::AdminVecs;
use log::info;

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct AdminLine {
    pub code: String,
    pub name: String,
    pub asciiname: String,
    pub geonameid: i64,
}

#[derive(Debug)]
pub struct AdminRec {
    pub id: i64,
    pub code: String,
    pub name: String,
}


pub async fn import_admins_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(&source_file_path)
                .map_err(|e| AppError::IoWriteErrorWithPath(e, source_file_path))?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);

        let mut i = 0;
        let vector_size = 5000;
        let mut dv: AdminVecs = AdminVecs::new(vector_size);
             
        for result in csv_rdr.deserialize() {
     
            let source: AdminLine = result?;

            let admin_rec = AdminRec {
                id: source.geonameid,
                code: source.code,
                name: source.name.trim().replace(".", "").replace("'", "’"),
            };

            dv.add_data(&admin_rec);   // transfer data to vectors
            i +=1;    

         }

         let mut table_name = "adm2s";
         if source_file_name.to_string().starts_with("admin1") {
            table_name = "adm1s";
         }
              
         dv.store_data(&pool, table_name).await?;
         info!("{} records processed from {} to src.{}", i, source_file_name, table_name);
                 
         Ok(())
}