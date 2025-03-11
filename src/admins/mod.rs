mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_admins_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists src.adm1s;
                create table src.adm1s
                (
                    id              int
                  , code            varchar   
                  , name            varchar  
                );"#;
   
    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"drop table if exists src.adm2s;
                create table src.adm2s
                (
                    id              int
                  , code            varchar   
                  , name            varchar  
                );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_admins_data(data_folder, source_file_name, pool).await

}