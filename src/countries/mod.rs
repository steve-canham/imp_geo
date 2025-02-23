mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_country_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.countries;
                create table geo.countries
                (
                    id                    int  primary key
                  , rank                  int
                  , iso_code              varchar
                  , country_name          varchar
                  , continent             varchar
                  , tld                   varchar
                  , capital               varchar
                );
                create index country_iso_code on geo.countries(iso_code);"#;

sqlx::raw_sql(sql).execute(pool)
.await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


let sql = r#"drop table if exists geo.country_names;
            create table geo.country_names
            (
                  id                    int
                , country_name          varchar
                , alt_name              varchar
                , langlist              varchar
                , source                varchar
            );
            create index country_name_alt_name on geo.country_names(alt_name);
            create index country_name_id on geo.country_names(id);"#;


sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

        Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool).await

}