use super::import::AdminRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct AdminVecs {
    pub ids: Vec<i64>,
    pub codes: Vec<String>,
    pub names: Vec<String>,
}


impl AdminVecs{
    pub fn new(vsize: usize) -> Self {
        AdminVecs { 
            ids: Vec::with_capacity(vsize),
            codes: Vec::with_capacity(vsize),
            names: Vec::with_capacity(vsize),
        }
    }

    pub fn add_data(&mut self, r: &AdminRec) 
    {
        self.ids.push(r.id);
        self.codes.push(r.code.clone());
        self.names.push(r.name.clone());
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>, table_name: &str) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO geo."#.to_string() + table_name + r#"(id, code, name) 
            SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[]);"#;

        sqlx::query(&sql)
        .bind(&self.ids).bind(&self.codes).bind(&self.names)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
