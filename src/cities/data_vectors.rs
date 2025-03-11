use super::import::CityRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct CityVecs {
    pub ids: Vec<i64>,
    pub names: Vec<String>,
    pub disamb_types: Vec<String>,
    pub disamb_codes: Vec<String>,
    pub country_codes: Vec<String>,
    pub lats: Vec<Option<f64>>,
    pub lngs: Vec<Option<f64>>,
}


impl CityVecs{
    pub fn new(vsize: usize) -> Self {
        CityVecs { 
            ids: Vec::with_capacity(vsize),
            names: Vec::with_capacity(vsize),
            disamb_types: Vec::with_capacity(vsize),
            disamb_codes: Vec::with_capacity(vsize),
            country_codes: Vec::with_capacity(vsize),
            lats: Vec::with_capacity(vsize),
            lngs: Vec::with_capacity(vsize),   
        }
    }

    pub fn add_data(&mut self, r: &CityRec) 
    {
        self.ids.push(r.id);
        self.names.push(r.name.clone());
        self.disamb_types.push(r.disamb_type.clone());
        self.disamb_codes.push(r.disamb_code.clone());
        self.country_codes.push(r.country_code.clone());
        self.lats.push(r.lat);
        self.lngs.push(r.lng);
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO geo.cities (id, name, disamb_type, disamb_code, country_code, lat, lng) 
            SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::float[], $7::float[]);"#;

        sqlx::query(&sql)
        .bind(&self.ids).bind(&self.names).bind(&self.disamb_types).bind(&self.disamb_codes)
        .bind(&self.country_codes).bind(&self.lats).bind(&self.lngs)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
