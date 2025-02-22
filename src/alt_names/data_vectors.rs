use super::import::AltRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct AltRecVecs {
    pub geo_ids: Vec<i64>,
    pub names: Vec<String>,
    pub langs: Vec<String>,
    pub historics: Vec<String>,
}


impl AltRecVecs{
    pub fn new(vsize: usize) -> Self {
        AltRecVecs { 
            geo_ids: Vec::with_capacity(vsize),
            names: Vec::with_capacity(vsize),
            langs: Vec::with_capacity(vsize),
            historics: Vec::with_capacity(vsize),
        }
    }

    pub fn add_data(&mut self, r: &AltRec) 
    {
        self.geo_ids.push(r.geo_id);
        self.names.push(r.name.clone());
        self.langs.push(r.lang.clone());
        self.historics.push(r.historic.clone());
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO geo.alt_src_names (geo_id, alt_name, lang, historic) 
            SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[])"#;

        sqlx::query(sql)
        .bind(&self.geo_ids).bind(&self.names).bind(&self.langs).bind(&self.historics)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
