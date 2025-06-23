mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_alt_name_table(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.alt_names;
    create table geo.alt_names
    (
        id           int   
      , alt_name 	 varchar  
      , langs        varchar
    );
    create index alt_names_idx on geo.alt_names(id);"#;
   
    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>, latin_only: bool) -> Result<(), AppError> {

    import::import_alt_name_data(data_folder, source_file_name, pool, latin_only).await

}