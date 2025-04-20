mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_scope_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists src.regions;
            create table src.regions
            (
                  id               int
                , feature_code     varchar
                , name             varchar
                , members          varchar
            );"#;

    sqlx::raw_sql(sql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_scope_data(data_folder, source_file_name, pool).await?;
    let sql = r#"SET client_min_messages TO NOTICE;"#;   // final command to DB

    sqlx::raw_sql(sql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    Ok(())
    
}

