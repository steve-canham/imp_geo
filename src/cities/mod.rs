mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

pub async fn create_city_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists geo.cities;
                create table geo.cities
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


    let sql = r#"drop table if exists geo.city_names;
            create table geo.city_names
            (
                  id                    int
                , city_name             varchar
                , disamb_id             int
                , disamb_name           varchar
                , country_id            int
                , country_name          varchar
                , alt_name              varchar
                , langlist              varchar
            );
            create index city_name_alt_name on geo.city_names(alt_name);
            create index city_name on geo.city_names(id);"#;


    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_cities_data(data_folder, source_file_name, pool).await?;
    update_cities_data(pool).await?;
    remove_dup_cities(pool).await?;
    create_city_names(pool).await?;
    add_missing_city_names(pool).await?;
    delete_dup_city_names(pool).await?;

    transfer_names_to_mdr(pool).await?;
    make_mdr_related_changes_1(pool).await

}


async fn update_cities_data(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"update geo.cities g
                 set country_id = null
                 where country_code = 'none'"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"update geo.cities g
                 set country_id = c.id,
                 country_name = c.country_name
                 from geo.countries c
                 where g.country_code = c.iso_code"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"update geo.cities g
        set disamb_code = null
        where disamb_code = 'none'"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"update geo.cities c
                 set disamb_id = a.id,
                 disamb_name = a.name 
                 from src.adm1s a
                 where c.disamb_code = a.code
                 and c.disamb_type = 'admin1'"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city records updated with admin1 details", res.rows_affected());

    let sql = r#"update geo.cities c
                 set disamb_id = a.id,
                 disamb_name = a.name 
                 from src.adm2s a
                 where c.disamb_code = a.code
                 and c.disamb_type = 'admin2'"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city records updated with admin2 details", res.rows_affected());

    Ok(())
}


async fn remove_dup_cities(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Deals with dup cities in same disamb area and country 

    let sql = r#"SET client_min_messages TO WARNING; 
                drop table if exists geo.temp_dup_cities;
                drop table if exists geo.temp_dup_city_matches;
                
                create table geo.temp_dup_cities as 
                select  
                name, country_name, disamb_name, count(id)
                from geo.cities 
                group by name, country_name, disamb_name
                having count(id) > 1;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;      

    let sql = r#"create table geo.temp_dup_city_matches as 
                select c.*, true as to_delete 
                from geo.cities c
                inner join geo.temp_dup_cities t
                on c.country_name = t.country_name
                and c.disamb_name = t.disamb_name
                and c.name = t.name
                order by c.name;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"update geo.temp_dup_city_matches m
                set to_delete = false
                from 
                (  select c.name, min(c.id) as min
                from geo.temp_dup_city_matches c
                group by c.name, c.country_name, c.disamb_name
                ) s
                where m.id = s.min;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

        let sql = r#"delete from geo.cities c
                using geo.temp_dup_city_matches m
                where c.id = m.id
                and m.to_delete = true;

                drop table geo.temp_dup_cities;
                drop table geo.temp_dup_city_matches;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} duplicate city records deleted (same country, name, disamb area)", res.rows_affected());

    Ok(())
}


async fn create_city_names(pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    let sql = r#"insert into geo.city_names (id, city_name, disamb_id, 
         disamb_name, country_id, country_name, alt_name, langlist)
         select c.id, c.name, c.disamb_id, c.disamb_name, c.country_id, 
         c.country_name, a.alt_name, a.langs
         from geo.cities c 
         inner join src.alt_names a
         on c.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} city name records created", res.rows_affected());

    Ok(())
}


async fn add_missing_city_names(pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    // It appears that in the geonames data about 4600 cities (out of a total of 143,000+)
    // do not have an alt_name matching the geoname city_name - this ensures that all
    // names are in the city_names table

    let sql = r#"SET client_min_messages TO WARNING; 
         drop table if exists geo.temp_city_match;
         
         create table geo.temp_city_match as 
         select *
         from geo.city_names
         where city_name = alt_name;"#;

    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
     
    let sql = r#"insert into geo.city_names
         (id, city_name, disamb_id, disamb_name, 
         country_id, country_name, alt_name)
         select distinct n.id, n.city_name, 
         n.disamb_id, n.disamb_name, n.country_id, 
         n.country_name, n.city_name as alt_name
         from geo.city_names n
         left join geo.temp_city_match m
         on n.id = m.id
         where m.id is null;
         
         drop table geo.temp_city_match;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} missing city default names added to the city_names table", res.rows_affected());

    Ok(())
}


async fn delete_dup_city_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Deals with dup city names in same disamb area and country 

    let sql = r#"SET client_min_messages TO WARNING; 
            drop table if exists geo.temp_dup_city_names;
            create table geo.temp_dup_city_names
            as
            select  
            country_name, alt_name, count(id)
            from geo.city_names 
            group by country_name, alt_name
            having count(id) > 1
            order by count(id) desc"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    // drop the duplicates where they are alt names
    // i.e. identify the name records that match duplicated
    // combinations and remove if they are not the main city name  

    let sql = r#"delete from geo.city_names n
            using geo.temp_dup_city_names d
            where d.country_name = n.country_name
            and d.alt_name = n.alt_name
            and d.alt_name <> n.city_name;
            
            SET client_min_messages TO WARNING; 
            drop table if exists geo.temp_dup_city_names;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    info!("{} duplicate non-default city name records deleted (same country, name)", res.rows_affected());

    Ok(())
}


async fn transfer_names_to_mdr(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"SET client_min_messages TO WARNING; 
    drop table if exists mdr.city_names;
    create table mdr.city_names
            (
                  id                    int
                , alt_name              varchar
                , city_name             varchar
                , disamb_id             int
                , disamb_name           varchar
                , country_id            int
                , country_name          varchar
                , source                varchar
            );
            create index city_name_id on mdr.city_names(id);
            create index city_name_alt_name on mdr.city_names(alt_name);
            create index city_name_name on mdr.city_names(city_name);

            
    insert into mdr.city_names (id, alt_name, city_name, disamb_id, 
            disamb_name, country_id, country_name, source) 
            select id, lower(alt_name), city_name, disamb_id, 
            disamb_name, country_id, country_name, 'geonames'
            from geo.city_names"#;

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    Ok(())
}


async fn make_mdr_related_changes_1(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // Below are MDR specific related changes, to suypport better matching....

    // to avoid some very strange city allocations

    let sql = r#"delete from mdr.city_names where alt_name = 'Chicago' and disamb_name = 'Ohio';
    delete from mdr.city_names where alt_name = 'New York' and disamb_name = 'Nebraska';"#;

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    // to add some additional city names used within the mdr
 
    let sql = r#"insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'new york', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names where city_name = 'New York City' and alt_name = 'new york city';

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'bucuresti', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name ='Bucharest' and alt_name = 'bucharest';

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'besancon', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name ='Besançon' and alt_name = 'besançon';

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'clermont ferrand', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Clermont-Ferrand' and alt_name = 'clermont-ferrand';"#; 

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    let sql = r#"insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'tubingen', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Tübingen' and alt_name = 'tübingen'; 

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'wuerzburg', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Würzburg' and alt_name = 'würzburg'; 

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'luebeck', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Lübeck' and alt_name = 'lübeck'; 

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'munchen', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Munich' and alt_name = 'munich';"#; 

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    let sql = r#"insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'kanagawa', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Yokohama' and alt_name = 'yokohama'; 

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'saint-petersburg', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Saint Petersburg' and alt_name = 'saint petersburg';

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'saint-petersburg', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'St. Petersburg' and alt_name = 'saint petersburg';

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'caba', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Buenos Aires' and country_name = 'Argentina' and alt_name = 'buenos aires'; 

    insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
    select id, 'ciudad autonoma de buenos aires', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
    from mdr.city_names
    where city_name = 'Buenos Aires' and country_name = 'Argentina' and alt_name = 'buenos aires';"#; 

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;    

    Ok(())
}



/*

insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
select 4735966, 'Temple, ambig_id, disambig_name, country_id, country_name, 'Temple', 'mdr'
from mdr.city_names
where city_name = 'Dallas' and alt_name = 'Dallas' 

insert into mdr.city_names(id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 5193342, 'Hershey', disambig_id, disambig_name, country_id, country_name, 'Hershey', 'mdr'
from mdr.city_names
where geoname_id = 5192726 and alt_name = 'Harrisburg'

insert into mdr.city_names(id, city_name, disamb_id, disamb_name, country_id, country_name, alt_name, source )
select 4460162, 'chapel hill', disamb_id, disamg_name, country_id, country_name, 'Chapel Hill', 'mdr'
from mdr.city_names
where geoname_id = 4464368 and alt_name = 'durham'



insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt-oder', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt / Oder', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt-oder', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt Oder', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt, Oder', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/ Oder Brandenburg', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt (Oder)' and alt_name = 'Frankfurt (Oder)'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt on the Main', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/ Main', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/a. M. -Höchst', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/Höchst', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/M', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, 'Frankfurt/M.', 'mdr'
from mdr.city_names
where city_name = 'Frankfurt am Main' and alt_name = 'Frankfurt am Main'


--also!

60590 Frankfurt    
D-60590 Frankfurt
Frankfurt
Frankfurt-am-Main
Frankfurt-Hochst
Frankfurt-Main
Frankfurt (a.M.)
Frankfurt (Main)
Frankfurt / Main
Frankfurt /Main
Frankfurt a Main
Frankfurt A. M
Frankfurt a. Main
Frankfurt a.M.
Frankfurt a.M. Hochst
Frankfurt a/M
Frankfurt am M.
Frankfurt Am Main Hessen
Frankfurt am Main,
Frankfurt am Mein
Frankfurt Hoechst
Frankfurt M
Frankfurt Main
Frankfurt N/A



insert into mdr.city_names(id, alt_name, city_name, disamb_id, disamb_name, country_id, country_name, source)
select id, 'praha 2', city_name, disamb_id, disamb_name, country_id, country_name, 'mdr'
from mdr.city_names
where city_name ='Prague' and alt_name = 'prague'

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, 'Prague', disambig_id, disambig_name, country_id, country_name, 'Prague 2', 'mdr'
from mdr.city_names
where city_name = 'Prague' and alt_name = 'Prague' 


insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select geoname_id, 'Prague', disambig_id, disambig_name, country_id, country_name, 'Praha 3', 'mdr'
from mdr.city_names
where city_name = 'Prague' and alt_name = 'Prague' 

--and
Praha 4
Praha 5
Praha 6
Praha 8
Praha 10


insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 4735966, 'Temple, ambig_id, disambig_name, country_id, country_name, 'Temple', 'mdr'
from mdr.city_names
where city_name = 'Dallas' and alt_name = 'Dallas' 

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 3074967, 'Hradec Králové, 3339540, 'Královéhradecký kraj', country_id, country_name, 'Temple', 'mdr'
from mdr.city_names
where city_name = 'Prague' and alt_name = 'Prague' 

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 4719457, 'Plano', ambig_id, disambig_name, country_id, country_name, 'Plano', 'mdr'
from mdr.city_names
where city_name = 'Dallas' and alt_name = 'Dallas' 

insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 4719457, 'Newport Beach', ambig_id, disambig_name, country_id, country_name, 'Newport Beach', 'mdr'
from mdr.city_names
where city_name = 'Irvine' and alt_name = 'Irvine'  and country_name = 'United States'


insert into mdr.city_names(geoname_id, city_name, disambig_id, disambig_name, country_id, country_name, alt_name, source )
select 500784, 'Royal Oak', ambig_id, disambig_name, country_id, country_name, 'Royal Oak', 'mdr'
from mdr.city_names
where city_name = 'Detroit' and alt_name = 'Detroit' 

*/

