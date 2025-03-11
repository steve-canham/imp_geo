mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

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
                , comp_name             varchar
                , langlist              varchar
                , source                varchar
            );
            create index country_name_comp_name on geo.country_names(comp_name);
            create index country_name_id on geo.country_names(id);"#;


sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

        Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool).await?;
    create_country_names(pool).await?;
    adjust_names(pool).await
}


async fn create_country_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into geo.country_names (id, country_name, alt_name, comp_name, langlist, source)
        select g.id, g.country_name, a.alt_name, 
        lower(replace(a.alt_name,'.', '')), 
        a.langs, 'geonames'
        from geo.countries g
        inner join src.alt_names a
        on g.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country name records created", res.rows_affected());

    Ok(())
}


async fn adjust_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // First add a few (currently 5) country names that are the name in the
    // countries table, but which do not seem to be present in the country_name list

    let sql = r#"insert into geo.country_names (id, country_name, alt_name, comp_name, langlist, source)
        select c.id, c.country_name, c.country_name, lower(replace(c.country_name,'.', '')), 
            '', 'geonames'
        from geo.countries c
        left join 
            (select cn.country_name from geo.country_names cn
            where country_name = alt_name
            order by country_name) m
        on c.country_name = m.country_name
        where m.country_name is null"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} missing country name records added", res.rows_affected());

    // Second remove three countries that for most purposes do not exist, from both 
    // country and country name records

    let sql = r#"delete from geo.countries 
              where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"delete from geo.country_names
            where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("Records for obsolete countries deleted");

    Ok(())
}











