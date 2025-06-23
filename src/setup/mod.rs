pub mod config_reader;
pub mod log_helper;
pub mod cli_reader;

use crate::err::AppError;
use sqlx::postgres::{PgPoolOptions, PgConnectOptions, PgPool};
use std::path::PathBuf;
use cli_reader::{CliPars, Flags};
use std::fs;
use std::time::Duration;
use sqlx::ConnectOptions;
use config_reader::Config;
use std::sync::OnceLock;

pub struct InitParams {
    pub data_folder: PathBuf,
    pub log_folder: PathBuf,
    pub flags: Flags,
}

pub static LOG_RUNNING: OnceLock<bool> = OnceLock::new();

pub fn get_params(cli_pars: CliPars, config_string: &String) -> Result<InitParams, AppError> {

    // Called from lib::run as the initial task of the program.
    // Returns a struct that contains the program's parameters.
      
    // Normal import and / or processing and / or outputting
    // If folder name also given in CL args the CL version takes precedence
    
    let config_file: Config = config_reader::populate_config_vars(&config_string)?; 
    let folder_pars = config_file.folders;  // guaranteed to exist

    let empty_pb = PathBuf::from("");
    let mut data_folder_good = true;

    let data_folder =  folder_pars.data_folder_path;
    if !folder_exists (&data_folder) 
    {   
        data_folder_good = false;
    }

    if !data_folder_good && cli_pars.flags.import_data { 
        return Result::Err(AppError::MissingProgramParameter("data_folder".to_string()));
    }

    let mut log_folder = folder_pars.log_folder_path;
    if log_folder == empty_pb && data_folder_good {
        log_folder = data_folder.clone();
    }
    else {
        if !folder_exists (&log_folder) { 
            fs::create_dir_all(&log_folder)?;
        }
    }
   
    // For execution flags read from the environment variables
    
    Ok(InitParams {
        data_folder,
        log_folder,
        flags: cli_pars.flags,
    })

}


fn folder_exists(folder_name: &PathBuf) -> bool {
    let xres = folder_name.try_exists();
    let res = match xres {
        Ok(true) => true,
        Ok(false) => false, 
        Err(_e) => false,           
    };
    res
}
        


pub async fn get_db_pool() -> Result<PgPool, AppError> {  

    // Establish DB name and thence the connection string
    // (done as two separate steps to allow for future development).
    // Use the string to set up a connection options object and change 
    // the time threshold for warnings. Set up a DB pool option and 
    // connect using the connection options object.

    let db_name = match config_reader::fetch_db_name() {
        Ok(n) => n,
        Err(e) => return Err(e),
    };

    let db_conn_string = config_reader::fetch_db_conn_string(&db_name)?;  
   
    let mut opts: PgConnectOptions = db_conn_string.parse()
                    .map_err(|e| AppError::DBPoolError("Problem with parsing conection string".to_string(), e))?;
    opts = opts.log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(3));

    PgPoolOptions::new()
        .max_connections(5) 
        .connect_with(opts).await
        .map_err(|e| AppError::DBPoolError(format!("Problem with connecting to database {} and obtaining Pool", db_name), e))
}


pub fn establish_log(params: &InitParams) -> Result<(), AppError> {

    if !log_set_up() {  // can be called more than once in context of integration tests
        log_helper::setup_log(&params.log_folder)?;
        LOG_RUNNING.set(true).unwrap(); // should always work
        log_helper::log_startup_params(&params);
    }
    Ok(())
}

pub fn log_set_up() -> bool {
    match LOG_RUNNING.get() {
        Some(_) => true,
        None => false,
    }
}


// Tests
#[cfg(test)]

mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn check_config_vars_read_correctly() {

        let config = r#"
[folders]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.flags.import_data, true);
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.data_folder, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.log_folder, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));

    }
   
    
    #[test]
    #[should_panic]
    fn check_wrong_data_folder_panics() {

        let config = r#"
[folders]
data_folder_path="C:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();
        
        let args : Vec<&str> = vec!["dummy target", "-r"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let _res = get_params(cli_pars, &config_string).unwrap();
    }
}

