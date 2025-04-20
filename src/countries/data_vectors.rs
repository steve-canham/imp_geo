use super::import::CountryRec;
use crate::AppError;
use sqlx::{postgres::PgQueryResult, Pool, Postgres};

pub struct CountryVecs {
    pub ids: Vec<i64>,
    pub ranks: Vec<i32>,
    pub iso_codes: Vec<String>,
    pub country_names: Vec<String>,
    pub continents: Vec<String>,
    pub tlds: Vec<String>,
    pub languages: Vec<String>,
    pub capitals: Vec<String>,
}

impl CountryVecs{
    pub fn new(vsize: usize) -> Self {
        CountryVecs { 
            ids: Vec::with_capacity(vsize),
            ranks: Vec::with_capacity(vsize),
            iso_codes: Vec::with_capacity(vsize),
            country_names: Vec::with_capacity(vsize),
            continents: Vec::with_capacity(vsize),
            tlds: Vec::with_capacity(vsize),
            languages: Vec::with_capacity(vsize),
            capitals: Vec::with_capacity(vsize),
        }
    }

    pub fn add_data(&mut self, r: &CountryRec) 
    {
        self.ids.push(r.id);
        self.ranks.push(r.rank);
        self.iso_codes.push(r.iso_code.clone());
        self.country_names.push(r.country_name.clone());
        self.continents.push(r.continent.clone());
        self.tlds.push(r.tld.clone());
        self.languages.push(r.languages.clone());
        self.capitals.push(r.capital.clone());
    }


    pub async fn store_data(&self, pool : &Pool<Postgres>) -> Result<PgQueryResult, AppError> {

        let sql = r#"INSERT INTO geo.countries (id, rank, iso_code, country_name, continent, tld, languages, capital) 
            SELECT * FROM UNNEST($1::int[], $2::int[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[]);"#;

        sqlx::query(&sql)
        .bind(&self.ids).bind(&self.ranks).bind(&self.iso_codes).bind(&self.country_names)
        .bind(&self.continents).bind(&self.tlds).bind(&self.languages).bind(&self.capitals)
        .execute(pool).await
        .map_err(|e| AppError::SqlxError(e, sql.to_string()))
    }
}
