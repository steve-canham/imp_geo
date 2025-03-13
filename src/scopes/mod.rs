mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

pub async fn create_scope_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists mdr.geo_scopes;
            create table mdr.geo_scopes
            (
                  id               int
                , type_id          int
                , rank             int 
                , name             varchar
                , code             varchar
                , parent_id        int
                , parent           varchar
                , members          varchar
            );"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"drop table if exists geo.regions;
            create table geo.regions
            (
                  id               int
                , feature_code     varchar
                , name             varchar
                , members          varchar
            );"#;

    sqlx::raw_sql(sql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_scope_data(data_folder, source_file_name, pool).await?;
    create_scope_data_1(pool).await?;
    create_scope_data_2(pool).await?;
    create_scope_data_3(pool).await?;

    let sql = r#"SET client_min_messages TO NOTICE;"#;   // final command to DB

    sqlx::raw_sql(sql).execute(pool)
            .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    Ok(())
    
}


async fn create_scope_data_1(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members)
                 select 1003, 1, 1, 'Global', 'GLB', null, null, null;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members)
                select id, 2, 1, name, null, 1003, 'Global', members 
                from geo.regions 
                where feature_code = 'CONT'
                order by name;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members)
                select id, 3, 1, name, null, null, null, members
                from geo.regions
                where feature_code = 'RGN'
                order by name;"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"delete from mdr.geo_scopes where name in 
              ('Bodenseeregion', 'Catalunya', 'Denakil', 'Great Lakes Region', 'Pomerania', 'Southern Cone', 'World Ocean');"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;


    let sql = r#"update mdr.geo_scopes set name = 'The Americas' where name = 'America';
                update mdr.geo_scopes set parent_id = 6255146, parent = 'Africa'
                where id in (12746360, 2219875, 7729886, 7729889, 11820342, 11820677, 7729887, 2425938, 12212804, 7729885, 9406051);
                update mdr.geo_scopes set parent_id = 1003, parent = 'Global'
                where id in (10861432, 10941926, 12225504, 7730009); 	
                update mdr.geo_scopes set parent_id = 6255148, parent = 'Europe'
                where id in (12216908, 12217933, 12217934, 12217848, 7729884, 2616167, 7729883, 2614165, 9408658, 9408659);
                update mdr.geo_scopes set parent_id = 6255151, parent = 'Oceania'
                where id in (12626210, 7729898, 7729899, 7729900, 7729901);		
                update mdr.geo_scopes set parent_id = 6255147, parent = 'Asia'
                where id in (7729893, 7729894, 1579995, 7729896, 7729895, 7729897, 12218088, 6957698, 305104, 615318, 6269133);
                update mdr.geo_scopes set parent_id = 6255149, parent = 'North America'
                where id in (7729891, 3496393, 3571219, 7729890, 7729892);"#;
    
    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())   
}


async fn create_scope_data_2(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"
            update mdr.geo_scopes set code = 'AF' where name = 'Africa';
            update mdr.geo_scopes set code = 'AS' where name = 'Asia';
            update mdr.geo_scopes set code = 'EU' where name = 'Europe';
            update mdr.geo_scopes set code = 'NA' where name = 'North America';
            update mdr.geo_scopes set code = 'OC' where name = 'Oceania';
            update mdr.geo_scopes set code = 'SA' where name = 'South America';
            update mdr.geo_scopes set code = 'AN' where name = 'Antarctica';
        
            update mdr.geo_scopes set code = 'MAGH' where name = 'Al Maghrib';
            update mdr.geo_scopes set code = 'GULF' where name = 'Arab Gulf Countries';
            update mdr.geo_scopes set code = 'ARAB' where name = 'Arabia';
            update mdr.geo_scopes set code = 'ARCT' where name = 'Arctic';
            update mdr.geo_scopes set code = 'AUST' where name = 'Australasia';
            update mdr.geo_scopes set code = 'AUNZ' where name = 'Australia and New Zealand';
            update mdr.geo_scopes set code = 'BALK' where name = 'Balkans';
            update mdr.geo_scopes set code = 'BTRG' where name = 'Baltic Region';
            update mdr.geo_scopes set code = 'BTST' where name = 'Baltic States';
            update mdr.geo_scopes set code = 'CARI' where name = 'Caribbean';
            update mdr.geo_scopes set code = 'CAUC' where name = 'Caucasus Region';
            update mdr.geo_scopes set code = 'CNAF' where name = 'Central Africa';
            update mdr.geo_scopes set code = 'CNAM' where name = 'Central America';
            update mdr.geo_scopes set code = 'CNAS' where name = 'Central Asia';
            update mdr.geo_scopes set code = 'CNEU' where name = 'Central Europe';
            update mdr.geo_scopes set code = 'EAAF' where name = 'Eastern Africa';
            update mdr.geo_scopes set code = 'EAAS' where name = 'Eastern Asia';
            update mdr.geo_scopes set code = 'EAEU' where name = 'Eastern Europe';
            update mdr.geo_scopes set code = 'HNAF' where name = 'Horn of Africa';
            update mdr.geo_scopes set code = 'G5SH' where name = 'G5 Sahel';
            update mdr.geo_scopes set code = 'INCH' where name = 'Indochina';
            update mdr.geo_scopes set code = 'LACA' where name = 'Latin America and the Caribbean';
            update mdr.geo_scopes set code = 'LEVA' where name = 'Levant';
            update mdr.geo_scopes set code = 'MELA' where name = 'Melanesia';
            update mdr.geo_scopes set code = 'MICR' where name = 'Micronesia';
            update mdr.geo_scopes set code = 'MDAM' where name = 'Middle America';
            update mdr.geo_scopes set code = 'MDEA' where name = 'Middle East';
            update mdr.geo_scopes set code = 'NORD' where name = 'Nordic';
            update mdr.geo_scopes set code = 'NEAF' where name = 'Northeast Africa';
            update mdr.geo_scopes set code = 'NTAF' where name = 'Northern Africa';
            update mdr.geo_scopes set code = 'NTAM' where name = 'Northern America';
            update mdr.geo_scopes set code = 'NTEU' where name = 'Northern Europe';
            update mdr.geo_scopes set code = 'POLA' where name = 'Polar Regions';
            update mdr.geo_scopes set code = 'PONS' where name = 'Polynesia';
            update mdr.geo_scopes set code = 'SAHL' where name = 'Sahel';
            update mdr.geo_scopes set code = 'SCAN' where name = 'Scandinavia';
            update mdr.geo_scopes set code = 'SEAS' where name = 'South Eastern Asia';
            update mdr.geo_scopes set code = 'STAF' where name = 'Southern Africa';
            update mdr.geo_scopes set code = 'STAS' where name = 'Southern Asia';
            update mdr.geo_scopes set code = 'STEU' where name = 'Southern Europe';
            update mdr.geo_scopes set code = 'SSAF' where name = 'Sub-Saharan Africa';
            update mdr.geo_scopes set code = 'AMCS' where name = 'The Americas';
            update mdr.geo_scopes set code = 'WIND' where name = 'West Indies';
            update mdr.geo_scopes set code = 'WSAF' where name = 'Western Africa';
            update mdr.geo_scopes set code = 'WSAS' where name = 'Western Asia';
            update mdr.geo_scopes set code = 'ASEU' where name = 'Western Europe';"#;


    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    

    let sql = r#"insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members)
                select id, 4, rank, country_name, iso_code, null, continent, null
                from geo.countries
                order by country_name;
                
                update mdr.geo_scopes g
                set parent_id = cs.id,
                parent = cs.name
                from 
                    (select id, code, name from                   
                    mdr.geo_scopes s 
                    where type_id = 2) cs
                where type_id = 4
                and g.parent = cs.code;"#;


    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    Ok(())   
}
        
        
async fn create_scope_data_3(pool: &Pool<Postgres>) -> Result<(), AppError> {

     
 let sql = r#"insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1253, 6, 1, 'European Union', 'EUUN', 6255148, 'Europe', 
            'AT, BE, HR, CY, CZ, DK, EE, FI, FR, DE, GR, HU, IS, IT, LV, LT, LU, MT, NL, PL, PT, RO, SK, SI, ES, SE');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1254, 6, 1, 'European Economic Area', 'EUEA', 6255148, 'Europe', 
                    'AT, BE, HR, CY, CZ, DK, EE, FI, FR, DE, GR, HU, IE, IS, IT, LI, LV, LT, LU, MT, NL, NO, PL, PT, RO, SK, SI, ES, SE');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1255, 6, 1, 'OECD countries', 'OECD', 1003, 'Global', 
                 'AU, AT, BE, CA, CL, CZ, DK, EE, FI, FR, DE, GR, HU, IE, IS, IL, IT, JP, KR, LV, LT, LU, MX, NL, NZ, NO, PL, PT, SK, SI, ES, SE, CH, TR, GB, US');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1258, 6, 2, 'British Commonwealth', 'BRCW', 1003, 'Global', 
                    'AG, KE, WS, AU, KI, SC, BS, LS, SL, BD, MW, SG, BB, MY, SB, BZ, MV, ZA, BW, MT, LK, BN, MU, SZ, CM, MZ, TO, CA, NA, TT, CY, NR, TV, DM, NZ, UG, FJ, NG, GB, GM, PK, TZ, GH, PG, VU, GD, KN, RW, GY, LC, ZM, VC, IN, JM, ZW');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1260, 6, 2, 'Francophone countries', 'FRPH', 1003, 'Global', 
                    'FR, BE, BF, BI, CM, CA, CF, TD, KM, CD, CG, DJ, GQ, GA, GN, HT, CI, LU, MG, ML, MC, NE, RW, SN, SC, CH, TG, VU');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1262, 6, 2, 'Indian Ocean Rim countries', 'IORC', 1003, 'Global', 
                 'AU, BD, KM, FR, IN, ID, IR, KE, MG, MY, MV, MU, MZ, OM, SC, SG, SO, ZA, LK, TZ, TH, AE, YE');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1263, 6, 2, 'German speaking countries', 'DEPH', 6255148, 'Europe', 
                 'DE, AT, CH, LI, LU');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1264, 6, 2, 'Spanish speaking countries', 'ESPH', 1003, 'Global', 
                    'AR, BO, CL, CO, CR, CU, DO, EC, ES, GQ, GT, HN, MX, NI, PE, PY, UY, VE');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1265, 6, 2, 'Portuguese speaking countries', 'PTPH', 1003, 'Global', 
                    'PT, BR, CV, AO, MZ, GW, ST, TL');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1266, 6, 2, 'Netherlands, Australia, Thailand', 'NATC', 1003, 'Global', 
                    'NL, AU, TH');
        insert into mdr.geo_scopes(id, type_id, rank, name, code, parent_id, parent, members) values(1261, 6, 2, 'African Union', 'AFUN', 6255146, 'Africa', 
                    'DZ, AO, BJ, BW, BF, BI, CM, CV, CF, TD, KM, CD, CG, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, CI, KE, LS, LR, LY, MG, RW, EH, ST, SN, SC, SL, SO, ZA, SS, TZ, TG, TN, UG, ZM, ZW');"#;


    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"SET client_min_messages TO WARNING; 
            drop table if exists mdr.temp_scope_membership;

            create table mdr.temp_scope_membership as 
            select code as scope_code, id as scope_id, name as scope_name, UNNEST(STRING_TO_ARRAY(members, ',')) as code, 1 as member_id, '' as member_name
            from mdr.geo_scopes
            where members is not null;

            update mdr.temp_scope_membership m
            set code = trim(code);"#;

    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"SET client_min_messages TO WARNING; 
        drop table if exists mdr.scope_membership;
        
        create table mdr.scope_membership as 
        select 10000 + ROW_NUMBER() OVER (order by scope_name, member_name) as id,
        m.scope_code, m.scope_id, m.scope_name, m.code, c.id as gid, c.country_name
        from mdr.temp_scope_membership m
        inner join geo.countries c
        on m.code = c.iso_code;

        drop table if exists mdr.temp_scope_membership;"#;

    sqlx::raw_sql(sql).execute(pool)
         .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"select count(*) from mdr.geo_scopes;"#;
    let res: i64 = sqlx::query_scalar(sql).fetch_one(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    info!("{} geographical scope records created", res);

    let sql = r#"select count(*) from mdr.scope_membership;"#;
    let res: i64 = sqlx::query_scalar(sql).fetch_one(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;
    
    info!("{} geographical scope membership records created", res);

    Ok(())
}
