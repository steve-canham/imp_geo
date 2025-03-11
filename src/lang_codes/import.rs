use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::LangCodeVecs;
use log::info;

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


pub async fn import_lang_code_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(&source_file_path)
                .map_err(|e| AppError::IoWriteErrorWithPath(e, source_file_path))?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .delimiter(9)
        .from_reader(buf_reader);

        let mut i = 0;
        let vector_size = 2500;
        let mut dv: LangCodeVecs = LangCodeVecs::new(vector_size);
             
        for result in csv_rdr.deserialize() {
     
            let source: LangCodeLine = result?;

            let lang_code_rec = LangCodeRec {
                c3: source.c3,
                c2: source.c2,
                c1: source.c1,
                name: source.name.trim().replace(".", "").replace("'", "’"),
            };

            dv.add_data(&lang_code_rec);   // transfer data to vectors
            i +=1;    

         }
              
         dv.store_data(&pool).await?;
         info!("{} records processed from {} to src.lang_codes", i, source_file_name);
                 
         Ok(())
}


