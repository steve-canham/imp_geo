mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

pub async fn create_country_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists loc.countries;
                create table loc.countries
                (
                    id                    int  primary key
                  , rank                  int
                  , iso_code              varchar
                  , country_name          varchar
                  , continent             varchar
                  , tld                   varchar
                  , capital               varchar
                );
                create index country_iso_code on loc.countries(iso_code);"#;

sqlx::raw_sql(sql).execute(pool)
.await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


let sql = r#"drop table if exists loc.country_names;
            create table loc.country_names
            (
                  id                    int
                , country_name          varchar
                , alt_name              varchar
                , langlist              varchar
                , source                varchar
            );
            create index country_name_alt_name on loc.country_names(alt_name);
            create index country_name_id on loc.country_names(id);"#;


sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

        Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool).await?;
    create_country_names(pool).await

}


async fn create_country_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into loc.country_names (id, country_name, alt_name, langlist, source)
        select g.id, g.country_name, a.alt_name, a.langs, 'geonames'
        from loc.countries g
        inner join geo.alt_names a
        on g.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country name records created", res.rows_affected());

    Ok(())
}




