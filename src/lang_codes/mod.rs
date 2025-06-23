mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_lang_code_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // initial call to DB, below, ensures schemas in place, reduces spurious warnings

    let sql = r#"SET client_min_messages TO WARNING;  
    create schema if not exists src;
    create schema if not exists geo;
    create schema if not exists mdr;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"drop table if exists geo.lang_src_codes;
                create table geo.lang_src_codes
                (
                    c3           varchar
                  , c2           varchar
                  , c1           varchar
                  , name         varchar
                );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"drop table if exists geo.lang_codes;
                create table geo.lang_codes
                (
                    code         varchar primary key
                  , name         varchar  
                  , code_type    varchar
                );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_lang_code_data(data_folder, source_file_name, pool).await?;
    transfer_data(pool).await?;
    delete_src_table(pool).await
}

pub async fn transfer_data(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into geo.lang_codes (code, name, code_type)
                select * from
                (select c2 as code, name, '639-2' as code_type
                from geo.lang_src_codes where c3 = '' and c2 not in ('frr', 'srn', 'syc', 'rup'))
                union
                (select c3 as code, name, '639-3' as code_type
                from geo.lang_src_codes where c3 <> '')
                union
                (select c1 as code, name, '639-1' as code_type
                from geo.lang_src_codes where c1 <> '')
                order by code"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn delete_src_table(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.lang_src_codes;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}



