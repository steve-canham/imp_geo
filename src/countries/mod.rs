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
                    , languages             varchar
                    , capital               varchar
                );
            create index country_iso_code on geo.countries(iso_code);
            
            drop table if exists src.countries;
                create table src.countries
                (
                      id                    int  primary key
                    , rank                  int
                    , iso_code              varchar
                    , country_name          varchar
                    , continent             varchar
                    , tld                   varchar
                    , languages             varchar
                    , capital               varchar
                );
            create index country_iso_code on src.countries(iso_code);

            drop table if exists src.country_names;
                create table src.country_names
                (
                      id                    int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001 INCREMENT BY 1) 
                    , country_id            int
                    , country_name          varchar
                    , alt_name              varchar
                    , langlist              varchar
                );
            create index country_name_country_id on src.country_names(country_id);"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool).await?;

    transfer_countries(pool).await?;
    create_country_names(pool).await?;
    adjust_names(pool).await?;

    Ok(())
}


async fn transfer_countries(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into src.countries (id, rank, iso_code, country_name,
              continent, tld, capital)
              select id, rank, iso_code, country_name,
              continent, tld, capital
              from geo.countries
              order by country_name;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
                .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country records transferred to geo schema", res.rows_affected());

    Ok(())
}


async fn create_country_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into src.country_names (country_id, country_name, alt_name, langlist)
        select g.id, g.country_name, a.alt_name, a.langs
        from src.countries g
        inner join geo.alt_names a
        on g.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country name records created", res.rows_affected());

    Ok(())
}


async fn adjust_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // First add a few (currently 5) country names that are the name in the
    // countries table, but which do not seem to be present in the country_name list

    let sql = r#"insert into src.country_names (country_id, country_name, alt_name, langlist)
        select c.id, c.country_name, c.country_name, ''
        from src.countries c
        left join 
            (select cn.country_name from src.country_names cn
             where country_name = alt_name
             order by country_name) m
        on c.country_name = m.country_name
        where m.country_name is null"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} missing country name records added", res.rows_affected());

    // Second remove three countries that for most purposes do not exist, from both 
    // country and country name records

    let sql = r#"delete from src.countries 
              where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"delete from src.country_names
            where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("Records for obsolete countries deleted");

    Ok(())
}


