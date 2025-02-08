/**********************************************************************************
* The setup module. Referenced in main by 'mod setup'.
* The two public modules allow integration tests to call into them, to give those
* tests the same DB conection pool and logging capability as the main library.
* The log established by log_helper seems to be available throughout the program
* via a suitable 'use' statement.
***********************************************************************************/

pub mod config_reader;
pub mod log_helper;
mod cli_reader;

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

use crate::error_defs::{AppError, CustomError};
//use crate::error_defs::AppError;
use chrono::NaiveDate;
use sqlx::postgres::{PgPoolOptions, PgConnectOptions, PgPool};
use log::error;
use std::path::PathBuf;
use std::ffi::OsString;
use std::fs;
use std::time::Duration;
use sqlx::ConnectOptions;
use config_reader::Config;
use cli_reader::Flags;

pub struct InitParams {
    pub data_folder: PathBuf,
    pub log_folder: PathBuf,
    pub output_folder: PathBuf,
    pub data_date: String,
    pub flags: Flags,
}

pub async fn get_params(args: Vec<OsString>, config_string: String) -> Result<InitParams, AppError> {

    // Called from main as the initial task of the program.
    // Returns a struct that contains the program's parameters.
    // Start by obtaining CLI arguments and reading parameters from .env file.
      
    let flags = cli_reader::fetch_valid_arguments(args)?;

    if flags.create_config || flags.initialise {

       // Any ror data and any other flags or arguments are ignored.

        Ok(InitParams {
            data_folder: PathBuf::new(),
            log_folder: PathBuf::new(),
            output_folder: PathBuf::new(),
            data_date: "".to_string(),
            flags: flags,
        })
    }
    else {

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

        if !data_folder_good && flags.import_data { 

            let msg = "Required data folder does not exists or is not accessible";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
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
        
        let config_date = &config_file.data_details.data_date;
        let data_date = match NaiveDate::parse_from_str(config_date, "%Y-%m-%d") {
            Ok(_) => config_date.to_string(),
            Err(_) => "".to_string(),
        };

        if data_date == "" && flags.import_data {   // Raise an AppError...required data is missing.
            let msg = "Data date not provided";
            let cf_err = CustomError::new(msg);
            return Result::Err(AppError::CsErr(cf_err));
        }

        // For execution flags read from the environment variables
       
        Ok(InitParams {
            data_folder,
            log_folder,
            output_folder,
            data_date,
            flags: flags,
        })

    }
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

    let db_conn_string = config_reader::fetch_db_conn_string(db_name)?;  
   
    let mut opts: PgConnectOptions = db_conn_string.parse()?;
    opts = opts.log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(3));

    match PgPoolOptions::new()
    .max_connections(5) 
    .connect_with(opts).await {
        Ok(p) => Ok(p),
        Err(e) => {
            error!("An error occured while creating the DB pool: {}", e);
            error!("Check the DB credentials and confirm the database is available");
            return Err(AppError::SqErr(e))
        },
    }
}




/* 
// Tests
#[cfg(test)]

mod tests {
    use super::*;
          
    // Ensure the parameters are being correctly extracted from the CLI arguments
    // The testing functions need to be async because of the call to get_params.
    // the test therefore uses the async version of the temp_env::with_vars function.
    // This function needs to be awaited to execute.
    // The closure is replaced by an explicitly async block rather than
    // a normal closure. Inserting '||' before or after the 'async' results
    // in multiple complaints from the compiler. The async block can also
    // be replaced by a separate async function and called explicitly.
 
  
    #[tokio::test]
    async fn check_env_vars_overwrite_blank_cli_values() {

        // Note that in most cases the folder path given must exist, and be 
        // accessible, or get_params will panic and an error will be thrown. 

        
            ("data_folder_path", Some("E:/ROR/data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("output_file_name", Some("results 25.json")),
            ("data_version", Some("v1.60")),
            ("data_date", Some("2025-12-11")),

        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.flags.import_ror, true);
            assert_eq!(res.flags.process_data, false);
            assert_eq!(res.flags.export_text, false);
            assert_eq!(res.flags.create_lookups, false);
            assert_eq!(res.flags.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/outputs"));
            assert_eq!(res.source_file_name, "v1.58 20241211.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 25.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.58");
            assert_eq!(res.data_date, "2024-12-11");
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_overwrite_env_values() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_version", Some("v1.59")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 27.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-r", "-p", "-t", "-x",
                                     "-f", "E:/ROR/data", "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.flags.import_ror, true);
            assert_eq!(res.flags.process_data, true);
            assert_eq!(res.flags.export_text, true);
            assert_eq!(res.flags.export_csv, true);
            assert_eq!(res.flags.create_lookups, false);
            assert_eq!(res.flags.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 27.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            assert_eq!(res.data_date, "2026-12-25");
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_with_i_flag() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/20241211 1.58 data")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 27.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-r", "-p", "-i", 
                                        "-f", "E:/ROR/data", "-d", "2026-12-25", "-s", "schema2 data.json"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.flags.import_ror, false);
            assert_eq!(res.flags.process_data, false);
            assert_eq!(res.flags.export_text, false);
            assert_eq!(res.flags.create_lookups,true);
            assert_eq!(res.flags.create_summary, true);
            assert_eq!(res.data_folder, PathBuf::new());
            assert_eq!(res.log_folder, PathBuf::new());
            assert_eq!(res.output_folder, PathBuf::new());
            assert_eq!(res.source_file_name, "".to_string());
            assert_eq!(res.output_file_name, "".to_string());
            assert_eq!(res.data_version, "".to_string());
            assert_eq!(res.data_date, "".to_string());
        }
       ).await;

    }


    #[tokio::test]
    async fn check_cli_vars_with_a_flag_and_new_win_folders() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:\\ROR\\20241211 1.58 data")),
            ("log_folder_path", Some("E:\\ROR\\some logs")),
            ("output_folder_path", Some("E:\\ROR\\dummy\\some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-a", "-f", "E:\\ROR\\data", 
                                       "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.flags.import_ror, true);
            assert_eq!(res.flags.process_data, true);
            assert_eq!(res.flags.export_text, true);
            assert_eq!(res.flags.create_lookups, false);
            assert_eq!(res.flags.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            assert_eq!(res.data_date, "2026-12-25");
        }
      ).await;

    }
    
    #[tokio::test]
    async fn check_cli_vars_with_a_flag_and_new_posix_folders() {

        // Note that the folder path given must exist, 
        // and be accessible, or get_params will panic
        // and an error will be thrown. 

        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/data")),
            ("log_folder_path", Some("E:/ROR/some logs 2")),
            ("output_folder_path", Some("E:/ROR/dummy 2/some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-a", "-f", "E:/ROR/data", 
                                       "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
    
            assert_eq!(res.flags.import_ror, true);
            assert_eq!(res.flags.process_data, true);
            assert_eq!(res.flags.export_text, true);
            assert_eq!(res.flags.create_lookups, false);
            assert_eq!(res.flags.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/data"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs 2"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy 2/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            assert_eq!(res.data_date, "2026-12-25");
        }
      ).await;

    }


    #[tokio::test]
    #[should_panic]
    async fn check_wrong_data_folder_panics_if_r() {
    
    temp_env::async_with_vars(
    [
        ("data_folder_path", Some("E:/ROR/20240607 1.47 data")),
        ("log_folder_path", Some("E:/ROR/some logs")),
        ("output_folder_path", Some("E:/ROR/dummy/some outputs")),
        ("src_file_name", Some("v1.58 20241211.json")),
        ("data_date", Some("2025-12-11")),
        ("output_file_name", Some("results 28.json")),
    ],
    async { 
        let args : Vec<&str> = vec!["target/debug/ror1.exe", "-a", "-f", "E:/silly folder name", 
                                    "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let _res = get_params(test_args).await.unwrap();
        }
      ).await;
    }

    #[tokio::test]
    async fn check_wrong_data_folder_does_not_panic_if_not_r() {
    
        temp_env::async_with_vars(
        [
            ("data_folder_path", Some("E:/ROR/daft data")),
            ("log_folder_path", Some("E:/ROR/some logs")),
            ("output_folder_path", Some("E:/ROR/dummy/some outputs")),
            ("src_file_name", Some("v1.58 20241211.json")),
            ("data_date", Some("2025-12-11")),
            ("output_file_name", Some("results 28.json")),
        ],
        async { 
            let args : Vec<&str> = vec!["target/debug/ror1.exe", "-p", "-f", "E:/ROR/silly folder name", 
                                        "-d", "2026-12-25", "-s", "schema2 data.json", "-v", "v1.60"];
            let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            let res = get_params(test_args).await.unwrap();
            assert_eq!(res.flags.import_ror, false);
            assert_eq!(res.flags.process_data, true);
            assert_eq!(res.flags.export_text, false);
            assert_eq!(res.flags.create_lookups, false);
            assert_eq!(res.flags.create_summary, false);
            assert_eq!(res.data_folder, PathBuf::from("E:/ROR/silly folder name"));
            assert_eq!(res.log_folder, PathBuf::from("E:/ROR/some logs"));
            assert_eq!(res.output_folder, PathBuf::from("E:/ROR/dummy/some outputs"));
            assert_eq!(res.source_file_name, "schema2 data.json");
            let lt = Local::now().format("%m-%d %H%M%S").to_string();
            assert_eq!(res.output_file_name, format!("results 28.json at {}.txt", lt));
            assert_eq!(res.data_version, "v1.60");
            assert_eq!(res.data_date, "2026-12-25");

            }
        ).await;
    }

}
*/
