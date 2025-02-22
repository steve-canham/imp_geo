mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_country_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.countries;
                create table geo.countries
                (
                    geoname_id            int  primary key
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
                  geoname_id            int
                , country_name          varchar
                , alt_name              varchar
                , langlist              varchar
                , source                varchar
            );
            create index country_name_alt_name on geo.country_names(alt_name);
            create index country_name_geoname_id on geo.country_names(geoname_id);"#;


sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    /* 
    let sql = r#"drop table if exists geo.country_info;
                create table ge0.country_info
                (
                  iso                   varchar
                , iso3                  varchar
                , iso_numeric           int
                , fip                   varchar
                , country               varchar
                , capital               varchar
                , area_sqkm             float
                , population            bigint
                , continent             varchar
                , tld                   varchar
                , currencycode          varchar
                , currencyname          varchar
                , phone                 varchar
                , postalcodeformat      varchar
                , postalcoderegex       varchar
                , languages             varchar
                , geonameid             int
                , neighbours            varchar
                , equivalentfipscode    varchar
                );"#;
   
    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
*/

        Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>, latin_only: bool) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool, latin_only).await

}