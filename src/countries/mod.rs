mod data_vectors;
mod import;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;
use log::info;

pub async fn create_country_tables(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"drop table if exists src.countries;
                create table src.countries
                (
                      id                    int  primary key
                    , rank                  int
                    , iso_code              varchar
                    , country_name          varchar
                    , continent             varchar
                    , tld                   varchar
                    , capital               varchar
                );
            create index country_iso_code on src.countries(iso_code);
            
            drop table if exists geo.countries;
                create table geo.countries
                (
                      id                    int  primary key
                    , rank                  int
                    , iso_code              varchar
                    , country_name          varchar
                    , continent             varchar
                    , tld                   varchar
                    , capital               varchar
                );
            create index country_iso_code on geo.countries(iso_code);

            drop table if exists geo.country_names;
                create table geo.country_names
                (
                      id                    int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001 INCREMENT BY 1) 
                    , country_id            int
                    , country_name          varchar
                    , alt_name              varchar
                    , langlist              varchar
                );
            create index country_name_country_id on geo.country_names(country_id);

            drop table if exists mdr.country_names;
                create table mdr.country_names
                (
                      id                    int PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001 INCREMENT BY 1) 
                    , comp_name             varchar
                    , country_id            int
                    , iso_code              varchar
                    , country_name          varchar
                    , continent             varchar
                    , source                varchar
                );
            create index country_name_comp_name on mdr.country_names(comp_name);"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())
}


pub async fn import_data(data_folder: &PathBuf, source_file_name: &str, pool: &Pool<Postgres>) -> Result<(), AppError> {

    import::import_countries_data(data_folder, source_file_name, pool).await?;

    transfer_countries(pool).await?;
    create_country_names(pool).await?;
    adjust_names(pool).await?;
    create_mdr_country_names(pool).await?;
    add_mdr_names_1(pool).await?;
    add_mdr_names_2(pool).await?; 
    add_mdr_names_3(pool).await?; 
    add_mdr_names_4(pool).await?; 
    add_mdr_names_5(pool).await 
}


async fn transfer_countries(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into geo.countries (id, rank, iso_code, country_name,
              continent, tld, capital)
              select id, rank, iso_code, country_name,
              continent, tld, capital
              from src.countries
              order by country_name;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
                .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country records transferred to geo schema", res.rows_affected());

    Ok(())
}


async fn create_country_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into geo.country_names (country_id, country_name, alt_name, langlist)
        select g.id, g.country_name, a.alt_name, a.langs
        from geo.countries g
        inner join src.alt_names a
        on g.id = a.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country name records created", res.rows_affected());

    Ok(())
}


async fn adjust_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // First add a few (currently 5) country names that are the name in the
    // countries table, but which do not seem to be present in the country_name list

    let sql = r#"insert into geo.country_names (country_id, country_name, alt_name, langlist)
        select c.id, c.country_name, c.country_name, ''
        from geo.countries c
        left join 
            (select cn.country_name from geo.country_names cn
             where country_name = alt_name
             order by country_name) m
        on c.country_name = m.country_name
        where m.country_name is null"#;

    let res = sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} missing country name records added", res.rows_affected());

    // Second remove three countries that for most purposes do not exist, from both 
    // country and country name records

    let sql = r#"delete from geo.countries 
              where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
        .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    let sql = r#"delete from geo.country_names
            where country_name in ('Serbia and Montenegro', 'Macao', 'Hong Kong')"#;

    sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("Records for obsolete countries deleted");

    Ok(())
}

async fn create_mdr_country_names(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let sql = r#"insert into mdr.country_names (comp_name, country_id, iso_code, 
    country_name, continent, source)
    select lower(replace(cn.alt_name,'.', '')), c.id, c.iso_code, 
    c.country_name, c.continent, 'geonames'
    from geo.country_names cn
    inner join geo.countries c
    on cn.country_id = c.id;"#;

    let res = sqlx::raw_sql(sql).execute(pool)
    .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    info!("{} country name records transferred to mdr schema", res.rows_affected());

    Ok(())
}


async fn add_mdr_names_1(pool: &Pool<Postgres>) -> Result<(), AppError> {

    // insert comp_name, country, 'mdr' as source
    // update mdr sourced records with country's id, iso_code, continent

    let params_set: Vec<Vec<&str>> = vec![
        vec!["aaland islands", "Aland Islands"],
        vec!["åland islands", "Aland Islands"],
        vec!["austlaria", "Australia"],
        vec!["austraria", "Australia"],
        vec!["austrarlia", "Australia"],
        vec!["autralia", "Australia"],
        vec!["bel", "Belgium"],
        vec!["bergium", "Belgium"],
        vec!["berugium", "Belgium"],
        vec!["beugium", "Belgium"],
        vec!["blgium", "Belgium"],
        vec!["bolivarian republic of", "Bolivia"],
        vec!["bonaire saint eustatius and saba", "Bonaire, Saint Eustatius and Saba"],
        vec!["sint eustatius and saba", "Bonaire, Saint Eustatius and Saba"],
        vec!["bosnial and herzegovina", "Bosnia and Herzegovina"],
        vec!["the republic of botswana", "Botswana"],
        vec!["brazi", "Brazil"],
        vec!["brazill", "Brazil"],
        vec!["diego garcia", "British Indian Ocean Territory"],
        vec!["virgin islands - british", "British Virgin Islands"],
        vec!["bulagaria", "Bulgaria"],
        vec!["camboia", "Cambodia"],
        vec!["canda", "Canada"],
        vec!["chilie", "Chile"],
    ];

    add_country_recs(params_set, pool).await?;


    let params_set: Vec<Vec<&str>> = vec![
        vec!["2/5000   china", "China"],
        vec!["bei jing", "China"],
        vec!["beijing", "China"],
        vec!["beijng", "China"],
        vec!["cchina", "China"],
        vec!["chain", "China"],
        vec!["chaina", "China"],
        vec!["chengdu", "China"],
        vec!["chhina", "China"],
        vec!["chia", "China"],
        vec!["chian", "China"],
        vec!["chiana", "China"],
        vec!["chiina", "China"],
        vec!["chin", "China"],
        vec!["chin''a", "China"],
        vec!["china hong cong", "China"],
        vec!["china hong kong", "China"],
        vec!["china mainland", "China"],
        vec!["china mainland (prc)", "China"],
        vec!["china pr", "China"],
        vec!["china?", "China"],
        vec!["china\\", "China"],
        vec!["china‘", "China"],
        vec!["china·", "China"],
        vec!["chinachina", "China"],
        vec!["chinese", "China"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["ching", "China"],
        vec!["chinia", "China"],
        vec!["chinna", "China"],
        vec!["chins", "China"],
        vec!["chna", "China"],
        vec!["chnia", "China"],
        vec!["chongqing", "China"],
        vec!["cinia", "China"],
        vec!["cna", "China"],
        vec!["cnina", "China"],
        vec!["guangdong", "China"],
        vec!["guangdung", "China"],
        vec!["guangxi zhuang autonomous region", "China"],
        vec!["guizhou province", "China"],
        vec!["hebei", "China"],
        vec!["henan", "China"],
        vec!["hina", "China"],
        vec!["hksar", "China"],
        vec!["hong kong", "China"],
        vec!["hong kong -  china", "China"],
        vec!["hong kong sar -  china", "China"],
        vec!["hong kong sar china", "China"],
        vec!["hongkong", "China"],
        vec!["hubei", "China"],
        vec!["jiangsu", "China"],
    ];

    add_country_recs(params_set, pool).await?;

    Ok(())
}


async fn add_mdr_names_2(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let params_set: Vec<Vec<&str>> = vec![
        vec!["macao", "China"],
        vec!["macao sar china", "China"],
        vec!["macau", "China"],
        vec!["p r china", "China"],
        vec!["people republic of china", "China"],
        vec!["pepole’s republic of china", "China"],
        vec!["pr china", "China"],
        vec!["pr of china", "China"],
        vec!["prc", "China"],
        vec!["prchina", "China"],
        vec!["shaanxi", "China"],
        vec!["shandong", "China"],
        vec!["shanghai", "China"],
        vec!["sichuan", "China"],
        vec!["the first affiliated hospital of hunan university of traditional chinese medicine", "China"],
        vec!["the people’s republic of china", "China"],
        vec!["tianjin", "China"],
        vec!["tibet and xinjiang", "China"],
        vec!["wuhan", "China"],
        vec!["zhangzhou", "China"],
        vec!["zhejiang", "China"],
        vec!["zhong guo", "China"],
        vec!["zhongguo", "China"],
        vec!["union of comoros", "Comoros"],
        vec!["chech republic", "Czechia"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["cze", "Czechia"],
        vec!["czech", "Czechia"],
        vec!["czech rep", "Czechia"],
        vec!["czech republicc", "Czechia"],
        vec!["czech rupublic", "Czechia"],
        vec!["czechi", "Czechia"],
        vec!["the czech republic", "Czechia"],
        vec!["zech republic", "Czechia"],
        vec!["congo -  democratic republic", "Democratic Republic of the Congo"],
        vec!["congo -  the democratic republic of the", "Democratic Republic of the Congo"],
        vec!["congo democratic republic", "Democratic Republic of the Congo"],
        vec!["the democratic republic of the congo", "Democratic Republic of the Congo"],
        vec!["zaire", "Democratic Republic of the Congo"],
        vec!["egypts", "Egypt"],
        vec!["ezypt", "Egypt"],
        vec!["the arab republic of egypt", "Egypt"],
        vec!["ethopia", "Ethiopia"],
        vec!["fra", "France"],
        vec!["french republic", "France"],
        vec!["german", "Germany"],
        vec!["germany etc", "Germany"],
        vec!["germay", "Germany"],
        vec!["gerymany", "Germany"],
        vec!["guinea-bisseu", "Guinea-Bissau"],
        vec!["hangary", "Hungary"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["hugary", "Hungary"],
        vec!["humgary", "Hungary"],
        vec!["hun", "Hungary"],
        vec!["inida", "India"],
        vec!["iran -  islamic republic of", "Iran"],
        vec!["kurdistan region of iraq", "Iraq"],
        vec!["irl", "Ireland"],
        vec!["israe", "Israel"],
        vec!["israelc", "Israel"],
        vec!["isreal", "Israel"],
        vec!["ita", "Italy"],
        vec!["itary", "Italy"],
        vec!["itay", "Italy"],
        vec!["cote divoire", "Ivory Coast"],
        vec!["japan only", "Japan"],
        vec!["japanetc", "Japan"],
        vec!["only in japan", "Japan"],
        vec!["only japan", "Japan"],
        vec!["democratic republic lau", "Laos"],
        vec!["latovia", "Latvia"],
        vec!["lebano", "Lebanon"],
        vec!["mex", "Mexico"],
        vec!["mexco", "Mexico"],
        vec!["republic of mexico", "Mexico"],
        vec!["micronesia (federated states of)", "Micronesia"],
    ];

    add_country_recs(params_set, pool).await?;

    Ok(())
}


async fn add_mdr_names_3(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let params_set: Vec<Vec<&str>> = vec![
        vec!["modalvia", "Moldova"],
        vec!["moldova - republic of", "Moldova"],
        vec!["republic of netherlands", "The Netherlands"],
        vec!["thenetherlands", "The Netherlands"],
        vec!["new zeland", "New Zealand"],
        vec!["newzealand", "New Zealand"],
        vec!["newzeland", "New Zealand"],
        vec!["democratic people republic of korea", "North Korea"],
        vec!["korea - democratic people’s republic of", "North Korea"],
        vec!["korea - north", "North Korea"],
        vec!["korea (the democratic peoples republic of)", "North Korea"],
        vec!["korea north", "North Korea"],
        vec!["macedonia - the former yugoslav republic of", "North Macedonia"],
        vec!["the former yugoslav rep of macedonia", "North Macedonia"],
        vec!["paraguar", "Paraguay"],
        vec!["peru rep", "Peru"],
        vec!["philippine", "Philippines"],
        vec!["phillipines", "Philippines"],
        vec!["philppines", "Philippines"],
        vec!["pol", "Poland"],
        vec!["poland rep", "Poland"],
        vec!["perto rico", "Puerto Rico"],
        vec!["puertorico", "Puerto Rico"],
        vec!["roman", "Romania"],
        vec!["russia federation", "Russia"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["russian", "Russia"],
        vec!["russian fed", "Russia"],
        vec!["russian fenderation", "Russia"],
        vec!["russianfederation", "Russia"],
        vec!["russina fed", "Russia"],
        vec!["rwanda", "Rwanda"],
        vec!["st vincent & grenadines", "Saint Vincent and the Grenadines"],
        vec!["santo tome and principe", "Sao Tome and Principe"],
        vec!["republic of serbia", "Serbia"],
        vec!["shingapore", "Singapore"],
        vec!["syngapore", "Singapore"],
        vec!["slovakia(slovak republic)", "Slovakia"],
        vec!["south afri", "South Africa"],
        vec!["south afric", "South Africa"],
        vec!["south georgia and the south sandwich is", "South Georgia and the South Sandwich Islands"],
        vec!["korea - republic of", "South Korea"],
        vec!["korea - south", "South Korea"],
        vec!["korea - republicof", "South Korea"],
        vec!["korea (republic of)", "South Korea"],
        vec!["korea (the republic of)", "South Korea"],
        vec!["korea republic", "South Korea"],
        vec!["korea republic of", "South Korea"],
        vec!["korea south", "South Korea"],
        vec!["rep korea", "South Korea"],
        vec!["rep of korea", "South Korea"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["republic of korean", "South Korea"],
        vec!["republic ofkorea", "South Korea"],
        vec!["rupublic of korea", "South Korea"],
        vec!["s korea", "South Korea"],
        vec!["sounth korea", "South Korea"],
        vec!["sourth korea", "South Korea"],
        vec!["sout korea", "South Korea"],
        vec!["south koreia", "South Korea"],
        vec!["south koresa", "South Korea"],
        vec!["south krea", "South Korea"],
        vec!["southkorea", "South Korea"],
        vec!["suoth korea", "South Korea"],
        vec!["the republic of korea", "South Korea"],
        vec!["canary islands", "Spain"],
        vec!["esp", "Spain"],
        vec!["spainm", "Spain"],
        vec!["spein", "Spain"],
        vec!["ascension and tristan da cunha", "St Helena"],
        vec!["sweden etc", "Sweden"],
        vec!["sweeded", "Sweden"],
        vec!["sweeden", "Sweden"],
        vec!["switzeland", "Switzerland"],
        vec!["switzeralnd", "Switzerland"],
        vec!["switzerand", "Switzerland"],
        vec!["switzerlad", "Switzerland"],
    ];

    add_country_recs(params_set, pool).await?;

    Ok(())

}


async fn add_mdr_names_4(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let params_set: Vec<Vec<&str>> = vec![
        vec!["swizerland", "Switzerland"],
        vec!["aiwan - province of china", "Taiwan"],
        vec!["taiwan - province of china", "Taiwan"],
        vec!["taiwan - taipei", "Taiwan"],
        vec!["taiwan - provinceof china", "Taiwan"],
        vec!["taiwan (province of china)", "Taiwan"],
        vec!["taiwan (roc)", "Taiwan"],
        vec!["taiwan china", "Taiwan"],
        vec!["taiwan province of china", "Taiwan"],
        vec!["taiwan roc", "Taiwan"],
        vec!["taiwian", "Taiwan"],
        vec!["tiwan", "Taiwan"],
        vec!["tayikistan", "Tajikistan"],
        vec!["tanzania -  united republic of", "Tanzania"],
        vec!["à¹thailand", "Thailand"],
        vec!["bangkok", "Thailand"],
        vec!["bankok", "Thailand"],
        vec!["hadyai", "Thailand"],
        vec!["hat yai", "Thailand"],
        vec!["hatyai", "Thailand"],
        vec!["maetha", "Thailand"],
        vec!["pathumthani", "Thailand"],
        vec!["tahiland", "Thailand"],
        vec!["tailand", "Thailand"],
        vec!["thai", "Thailand"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["thai land", "Thailand"],
        vec!["thaialnd", "Thailand"],
        vec!["thaiand", "Thailand"],
        vec!["thailan", "Thailand"],
        vec!["thailaned", "Thailand"],
        vec!["thailannd", "Thailand"],
        vec!["thailinad", "Thailand"],
        vec!["thailland", "Thailand"],
        vec!["thailnad", "Thailand"],
        vec!["thainland", "Thailand"],
        vec!["thaland", "Thailand"],
        vec!["thaniland", "Thailand"],
        vec!["thaoland", "Thailand"],
        vec!["thauland", "Thailand"],
        vec!["yasothon", "Thailand"],
        vec!["nerherland", "The Netherlands"],
        vec!["nertherlands", "The Netherlands"],
        vec!["nethaerlands", "The Netherlands"],
        vec!["nethelands", "The Netherlands"],
        vec!["netherland", "The Netherlands"],
        vec!["the republic of togo", "Togo"],
        vec!["trinidad&tobago", "Trinidad and Tobago"],
        vec!["tã¼rkiye", "Turkey"],
        vec!["trukey", "Turkey"],
        vec!["turke", "Turkey"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["turkey etc", "Turkey"],
        vec!["turkiye", "Turkey"],
        vec!["turks & caicos islands", "Turks and Caicos islands"],
        vec!["ukrine", "Ukraine"],
        vec!["england", "United Kingdom"],
        vec!["gbr", "United Kingdom"],
        vec!["inited kingdom", "United Kingdom"],
        vec!["kingdom of great britain and northern ireland", "United Kingdom"],
        vec!["northern ireland", "United Kingdom"],
        vec!["scotland", "United Kingdom"],
        vec!["the uk", "United Kingdom"],
        vec!["the united kingdom", "United Kingdom"],
        vec!["un of great britain and northern ireland", "United Kingdom"],
        vec!["unidet kingdum", "United Kingdom"],
        vec!["unite kingdom", "United Kingdom"],
        vec!["united kindgdom", "United Kingdom"],
        vec!["united kindom", "United Kingdom"],
        vec!["united kingdam", "United Kingdom"],
        vec!["united kingdo", "United Kingdom"],
        vec!["united kingdom and roi", "United Kingdom"],
        vec!["united kingdom of britain", "United Kingdom"],
        vec!["united kingdom of great britain", "United Kingdom"],
        vec!["united kingdom of great britain and northe", "United Kingdom"],
        vec!["united kingdom of great britain and northern irel", "United Kingdom"],
        vec!["united kingdom of great britain and northern irela", "United Kingdom"],
    ];

    add_country_recs(params_set, pool).await?;

    Ok(())

}


async fn add_mdr_names_5(pool: &Pool<Postgres>) -> Result<(), AppError> {

    let params_set: Vec<Vec<&str>> = vec![
        vec!["united kingdom of great britainand northern irelan", "United Kingdom"],
        vec!["united kingdum", "United Kingdom"],
        vec!["united of kingdom", "United Kingdom"],
        vec!["unitedkingdom", "United Kingdom"],
        vec!["unitedkingdom of great britain", "United Kingdom"],
        vec!["unitedkingdom of great britain and northern irelan", "United Kingdom"],
        vec!["unitedkingdom of great britain and northernireland", "United Kingdom"],
        vec!["unitedkingdom of great britainandnorthernireland", "United Kingdom"],
        vec!["unitedkingdomof greatbritainandnorthernireland", "United Kingdom"],
        vec!["unitred kingdom", "United Kingdom"],
        vec!["wales", "United Kingdom"],
        vec!["american", "United States"],
        vec!["the united states", "United States"],
        vec!["the united states of america", "United States"],
        vec!["the us", "United States"],
        vec!["unitd states", "United States"],
        vec!["united sates", "United States"],
        vec!["united staes", "United States"],
        vec!["united stat", "United States"],
        vec!["united state", "United States"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["united states of americ", "United States"],
        vec!["united states of amerika", "United States"],
        vec!["united states ofamerica", "United States"],
        vec!["united status", "United States"],
        vec!["unitedstates of america", "United States"],
        vec!["unitedstates ofamerica", "United States"],
        vec!["unites states", "United States"],
        vec!["us", "United States"],
        vec!["usa (including puerto rico)", "United States"],
        vec!["nitedstates of america", "United States"],
        vec!["virgin islands - us", "US Virgin Islands"],
        vec!["venezuela (bolivarian republic of)", "Venezuela"],
        vec!["socialist republic of viet nam", "Vietnam"],
        vec!["viet", "Vietnam"],
    ];

    add_country_recs(params_set, pool).await?;

    let params_set: Vec<Vec<&str>> = vec![
        vec!["africa", "AF"],
        vec!["africa region", "AF"],
        vec!["and asia (including china and japan)", "AS"],
        vec!["asia", "AS"],
        vec!["east asia(taiwan -  hong kong -  singapore and other)", "AS"],
        vec!["middle east", "AS"],
        vec!["approximately 30 countries in europe", "EU"],
        vec!["eu", "EU"],
        vec!["europe", "EU"],
        vec!["european", "EU"],
        vec!["european region", "EU"],
        vec!["european union", "EU"],
        vec!["former serbia and montenegro", "EU"],
        vec!["former yugoslavia", "EU"],
        vec!["nordic countries", "EU"],
        vec!["serbia and montenegro", "EU"],
        vec!["yugoslavia", "EU"],
        vec!["central america", "NA"],
        vec!["including united states and canada", "NA"],
        vec!["north", "NA"],
        vec!["north america", "NA"],
        vec!["oceania", "OC"],
        vec!["south america", "SA"],
    ];

    add_continent_recs(params_set, pool).await?;

    Ok(())

}


async fn add_country_recs(params: Vec<Vec<&str>>, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let mut sql = "".to_string();
    for p in params {
         
         let this_sql = r#"insert into mdr.country_names(comp_name, country_name, source) 
                "#.to_string()
                + &format!("values('{}', '{}', 'mdr');", p[0], p[1]) + r#"

                "#;

         sql.push_str(&this_sql);
    }

    sqlx::raw_sql(&sql).execute(pool)
             .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())

}


async fn add_continent_recs(params: Vec<Vec<&str>>, pool: &Pool<Postgres>) -> Result<(), AppError> {

    let mut sql = "".to_string();
    for p in params {
         
         let this_sql = r#"insert into mdr.country_names(comp_name, continent, source) 
                "#.to_string()
                + &format!("values('{}', '{}', 'mdr');", p[0], p[1]) + r#"

                "#;

         sql.push_str(&this_sql);
    }

    sqlx::raw_sql(&sql).execute(pool)
             .await.map_err(|e| AppError::SqlxError(e, sql.to_string()))?;

    Ok(())

}



