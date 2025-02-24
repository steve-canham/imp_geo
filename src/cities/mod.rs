mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

pub async fn create_city_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists loc.cities;
                create table loc.cities
                (
                    id                    int primary key
                  , name                  varchar
                  , disamb_type           varchar
                  , disamb_id             int
                  , disamb_code           varchar
                  , disamb_name           varchar
                  , country_id            int
                  , country_code          varchar
                  , country_name          varchar
                  , lat                   float
                  , lng                   float
                );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"drop table if exists loc.city_names;
            create table loc.city_names
            (
                  id                    int
                , city_name             varchar
                , disamb_id             int
                , disamb_name           varchar
                , country_id            int
                , country_name          varchar
                , alt_name              varchar
                , langlist              varchar
                , source                varchar
            );
            create index city_name_alt_name on loc.city_names(alt_name);
            create index city_name on loc.city_names(id);"#;


    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_cities_data(data_folder, source_file_name, pool).await?;
    update_cities_data(pool).await?;
    create_city_names(pool).await

}


async fn update_cities_data(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"update loc.cities g
                 set country_id = null
                 where country_code = 'none'"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"update loc.cities g
                 set country_id = c.id,
                 country_name = c.country_name
                 from loc.countries c
                 where g.country_code = c.iso_code"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"update loc.cities g
        set disamb_code = null
        where disamb_code = 'none'"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"update loc.cities c
                 set disamb_id = a.id,
                 disamb_name = a.name 
                 from geo.adm1s a
                 where c.disamb_code = a.code
                 and c.disamb_type = 'admin1'"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city records updated with admin1 details", res.rows_affected());

    let sql = r#"update loc.cities c
                 set disamb_id = a.id,
                 disamb_name = a.name 
                 from geo.adm2s a
                 where c.disamb_code = a.code
                 and c.disamb_type = 'admin2'"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city records updated with admin2 details", res.rows_affected());

    Ok(())
}


async fn create_city_names(pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql = r#"insert into loc.city_names (id, city_name, disamb_id, 
         disamb_name, country_id, country_name, alt_name, langlist, source)
         select c.id, c.name, c.disamb_id, c.disamb_name, c.country_id, 
         c.country_name, a.alt_name, a.langs, 'geonames'
         from loc.cities c 
         inner join geo.alt_names a
         on c.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city name records created", res.rows_affected());

    Ok(())
}
