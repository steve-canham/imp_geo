use super::import::LangCodeRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct LangCodeVecs {
    pub c3s: Vec<String>,
    pub c2s: Vec<String>,
    pub c1s: Vec<String>,
    pub names: Vec<String>,
}


impl LangCodeVecs{
    pub fn new(vsize: usize) -> Self {
        LangCodeVecs { 
            c3s: Vec::with_capacity(vsize),
            c2s: Vec::with_capacity(vsize),
            c1s: Vec::with_capacity(vsize),
            names: Vec::with_capacity(vsize),
        }
    }

    pub fn add_data(&mut self, r: &LangCodeRec) 
    {
        self.c3s.push(r.c3.clone());
        self.c2s.push(r.c2.clone());
        self.c1s.push(r.c1.clone());
        self.names.push(r.name.clone());
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO src.lang_src_codes (c3, c2, c1, name) 
            SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[]);"#;

        sqlx::query(&sql)
        .bind(&self.c3s).bind(&self.c2s).bind(&self.c1s).bind(&self.names)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
