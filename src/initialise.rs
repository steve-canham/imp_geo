use sqlx::{postgres::PgQueryResult, Pool, Postgres};
use crate::AppError;

pub async fn create_geo_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    execute_sql(get_alt_names_sql(), pool).await?;
   
    Ok(())
}


async fn execute_sql(sql: &str, pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    sqlx::raw_sql(&sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


fn get_alt_names_sql <'a>() -> &'a str {
    r#"drop table if exists geo.alt_names;
    create table geo.alt_names
    (
        id           int   
      , alt_name 	 varchar  
      , langs        varchar
      , historic     varchar
    );
    create index alt_names_idx on geo.alt_names(id);"#
}