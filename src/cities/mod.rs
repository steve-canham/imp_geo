mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn create_city_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.city_info;
                create table geo.city_info
                (
                    geonameid        int   
                  , name             varchar  
                  , asciiname        varchar  
                  , alternatenames   varchar 
                  , latitude         float   
                  , longitude        float 
                  , feature_class    varchar  
                  , feature_code     varchar  
                  , country_code     varchar  
                  , cc2              varchar 
                  , admin1_code      varchar 
                  , admin2_code      varchar 
                  , admin3_code      varchar 
                  , admin4_code      varchar
                  , population       bigint 
                  , elevation        int 
                  , dem              int 
                  , timezone         varchar
                  , mod_date         date
                );"#;
   
    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>, latin_only: bool) -> Result<(), AppError> {

    import::import_cities_data(data_folder, source_file_name, pool, latin_only).await

}