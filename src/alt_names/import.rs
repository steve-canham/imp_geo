use sqlx::{postgres::PgQueryResult, Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File;
use csv::ReaderBuilder;
use super::data_vectors::AltRecVecs;
use log::info;


#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct AltName {
    alternate_name_id: i64,
    geoname_id: i64,
    iso_language: Option<String>,
    alternate_name: String,
    is_preferred_name: Option<usize>,
    is_short_name: Option<usize>,
    is_colloquial: Option<usize>,
    is_historic: Option<usize>,
    yfrom: Option<String>,
    yto: Option<String>,
}

#[derive(Debug)]
pub struct AltRec {
    pub geo_id: i64,
    pub name: String,
    pub lang: String,
    pub historic: String,
}


pub async fn import_alt_name_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>, latin_only: bool) -> Result<(), AppError> {

    let source_file_path: PathBuf = [data_folder, &PathBuf::from(source_file_name)].iter().collect();
    let file = File::open(&source_file_path)
               .map_err(|e| AppError::IoWriteErrorWithPath(e, source_file_path))?;
    let buf_reader = BufReader::new(file);
    let mut csv_rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(9)
        .from_reader(buf_reader);
    
   let mut i = 0;
   let mut gid_num = 0;
   let mut old_gid = 0;

   let none = "none".to_string();
   let link = "link".to_string();
   let wkdt = "wkdt".to_string();
   let post = "post".to_string();
   let unlc = "unlc".to_string();
   let faac = "faac".to_string();
   let icao = "icao".to_string();
   let iata = "iata".to_string();
   let abbr = "abbr".to_string();
   let end_of_latin = "ZZ".to_string();

   let vector_size = 5000;
   let mut dv: AltRecVecs = AltRecVecs::new(vector_size);
   create_collecting_table(&pool).await?;

   for result in csv_rdr.deserialize() {

        let source: AltName = result?;
        let mut create_rec = true;
        let lang_code = source.iso_language.unwrap_or(none.clone());
        
        if lang_code != none {
            if lang_code == link || lang_code == wkdt
            || lang_code == post || lang_code == unlc
            || lang_code == faac || lang_code == icao
            || lang_code == iata || lang_code == abbr
            {
                create_rec = false;
            }
        }

        // Optional filter here to exclude non Latin names.
        
        if latin_only {
            if source.alternate_name > end_of_latin {
                create_rec = false;
            }
        }

        if create_rec {

            let geo_id = source.geoname_id;
            if geo_id != old_gid {
                gid_num = gid_num + 1;

                if gid_num == 2500 {  // every 2500 geoname ids

                    // Call the routine to transfer records to the database.
                    // Recreate the vectors, reset gid_num.
                    // Then aggregate lang codes and recreate the collecting table.

                    dv.store_data(&pool).await?;
                    dv = AltRecVecs::new(vector_size);
                    gid_num = 0;
                    transfer_data(&pool).await?;
                    create_collecting_table(&pool).await?;
                }
                
                old_gid = geo_id;
            }

            let mut is_historic = "".to_string();
            if source.is_historic.is_some() && source.is_historic.unwrap() == 1 {
                is_historic = "Historic".to_string();
                 if source.yfrom.is_some() {
                    is_historic = is_historic + ", from " + &source.yfrom.unwrap();
                 }
                 if source.yto.is_some() {
                    is_historic = is_historic + ", to " + &source.yto.unwrap();
                  }
            }
           
            let alt_name = AltRec {
                geo_id: source.geoname_id,
                name: source.alternate_name.trim().replace(".", "").replace("'", "’"),
                lang: lang_code.clone(),
                historic: is_historic,
            };

            // transfer data to vectors
            dv.add_data(&alt_name);
        }
        
        i +=1;

        //if i > 260000 {
        //    break;
        // }

        if i % 250000 == 0 {
            info!("Processed {} alternate name records", i);
        }
    }

    dv.store_data(&pool).await?;
    transfer_data(&pool).await?;
    info!("Processed {} alternate name records in total", i);
            
    Ok(())
}


async fn create_collecting_table(pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError> {
    
    let sql = r#"drop table if exists geo.alt_src_names;
    create table geo.alt_src_names
    (
        geo_id       int
      , alt_name     varchar
      , lang         varchar
      , historic     varchar
    );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}


async fn transfer_data(pool: &Pool<Postgres>) -> Result<PgQueryResult, AppError>  {


    let sql = r#"insert into geo.alt_names (id, alt_name, langs, historic)
        select geo_id, alt_name,
	    string_agg(c.name, ', '), historic
        from geo.alt_src_names n
        left join 
            (select * from geo.lang_codes where code_type = '639-1') c
        on n.lang = c.code
        group by geo_id, alt_name, historic
        order by geo_id, alt_name"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))
}

        