/**********************************************************************************
* The setup module. Referenced in main by 'mod setup'.
* The two public modules allow integration tests to call into them, to give those
* tests the same DB conection pool and logging capability as the main library.
* The log established by log_helper seems to be available throughout the program
* via a suitable 'use' statement.
***********************************************************************************/

pub mod config_reader;
pub mod log_helper;
pub mod cli_reader;

/**********************************************************************************
* This over-arching 'mod' setup module 
* a) establishes the final collection of parameters, taking into account both 
* environmental and CLI values. 
* b) Unpacks the file name to obtain data version and date, if possible, 
* c) Obtains a database connection pool 
* d) Orchestrates the creation of the lookup and summary schemas.
* It has a collection of unit tests ensuring that the parameter generatiuon process 
* is correct as well as some tests on the regex expression used on the source file.
***********************************************************************************/

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
    pub output_folder: PathBuf,
    pub source_file_name: String,
    pub flags: Flags,
}

pub static LOG_RUNNING: OnceLock<bool> = OnceLock::new();

pub fn get_params(cli_pars: CliPars, config_string: &String) -> Result<InitParams, AppError> {

    // Called from lib::run as the initial task of the program.
    // Returns a struct that contains the program's parameters.
      
    // Normal import and / or processing and / or outputting
    // If folder name also given in CL args the CL version takes precedence
    
    let config_file: Config = config_reader::populate_config_vars(&config_string)?; 
    let file_pars = config_file.files;  // guaranteed to exist

    let empty_pb = PathBuf::from("");
    let mut data_folder_good = true;

    let data_folder =  file_pars.data_folder_path;
    if !folder_exists (&data_folder) 
    {   
        data_folder_good = false;
    }

    if !data_folder_good && cli_pars.flags.import_data { 
        return Result::Err(AppError::MissingProgramParameter("data_folder".to_string()));
    }

    let mut log_folder = file_pars.log_folder_path;
    if log_folder == empty_pb && data_folder_good {
        log_folder = data_folder.clone();
    }
    else {
        if !folder_exists (&log_folder) { 
            fs::create_dir_all(&log_folder)?;
        }
    }

    let mut output_folder = file_pars.output_folder_path;
    if output_folder == empty_pb && data_folder_good {
        output_folder = data_folder.clone();
    }
    else {
        if !folder_exists (&output_folder) { 
            fs::create_dir_all(&output_folder)?;
        }
    }

    // If source file name given in CL args the CL version takes precedence.

    let mut source_file_name = cli_pars.source_file;
    if source_file_name == "".to_string(){
        source_file_name =  file_pars.src_file_name;
        if source_file_name == "".to_string() && cli_pars.flags.import_data {   // Required data is missing - Raise error and exit program.
            return Result::Err(AppError::MissingProgramParameter("src_file_name".to_string()));
        }
    }

    // For execution flags read from the environment variables
    
    Ok(InitParams {
        data_folder,
        log_folder,
        output_folder,
        source_file_name,
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
[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"
src_file_name="alternateNamesV2.txt"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.flags.import_data, true);
        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.data_folder, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.log_folder, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.output_folder, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));
        assert_eq!(res.source_file_name, "alternateNamesV2.txt");

    }

    #[test]
    fn check_cli_vars_overwrite_env_values() {

       let config = r#"
[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"
src_file_name="alternateNamesV2.txt"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target",  "-s", "schema2 data.txt"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.flags.import_data, true);
        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.data_folder, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.log_folder, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.output_folder, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));
        assert_eq!(res.source_file_name, "schema2 data.txt");
    }
    
    #[test]
    #[should_panic]
    fn check_wrong_data_folder_panics() {

        let config = r#"
[files]
data_folder_path="C:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"
src_file_name="alternateNamesV2.txt"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
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

