use super::import::ScopeRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct ScopeVecs {
    pub ids: Vec<i64>,
    pub feature_codes: Vec<String>,
    pub names: Vec<String>,
    pub members: Vec<Option<String>>,
}

impl ScopeVecs{
    pub fn new(vsize: usize) -> Self {
        ScopeVecs { 
            ids: Vec::with_capacity(vsize),
            feature_codes: Vec::with_capacity(vsize),
            names: Vec::with_capacity(vsize),
            members: Vec::with_capacity(vsize),
        }
    }

    pub fn add_data(&mut self, r: &ScopeRec) 
    {
        self.ids.push(r.id);
        self.feature_codes.push(r.feature_code.clone());
        self.names.push(r.name.clone());
        self.members.push(r.members.clone());
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO src.regions (id, feature_code, name, members) 
            SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[]);"#;

        sqlx::query(&sql)
        .bind(&self.ids).bind(&self.feature_codes).bind(&self.names).bind(&self.members)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
