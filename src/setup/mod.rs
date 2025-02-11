/**********************************************************************************
The setup module, and the get_params function in this file in particular, 
orchestrates the collection and fusion of parameters as provided in 
1) a config toml file, and 
2) command line arguments. 
Where a parameter may be given in either the config file or command line, the 
command line version always over-writes anything from the file.
The module also checks the parameters for completeness (those required will vary, 
depending on the activity specified). If possible, defaults are used to stand in for 
mising parameters. If not possible the program stops with a message explaining the 
problem.
The module also provides a database connection pool on demand.
***********************************************************************************/

pub mod config_reader;
pub mod log_helper;
mod cli_reader;

use crate::AppError;
use chrono::NaiveDate;
use sqlx::postgres::{PgPoolOptions, PgConnectOptions, PgPool};
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


pub fn get_params(args: Vec<OsString>, config_string: String) -> Result<InitParams, AppError> {

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
        
        let config_date = &config_file.data_details.data_date;
        let data_date = match NaiveDate::parse_from_str(config_date, "%Y-%m-%d") {
            Ok(_) => config_date.to_string(),
            Err(_) => "".to_string(),
        };

        if data_date == "" && flags.import_data {   
            return Result::Err(AppError::MissingProgramParameter("Data date".to_string()));
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

    let db_conn_string = config_reader::fetch_db_conn_string(&db_name)?;  
   
    let mut opts: PgConnectOptions = db_conn_string.parse()
                    .map_err(|e| AppError::DBPoolError("Problem with parsing conection string".to_string(), e))?;
    opts = opts.log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(3));

    PgPoolOptions::new()
        .max_connections(5) 
        .connect_with(opts).await
        .map_err(|e| AppError::DBPoolError(format!("Problem with connecting to database {} and obtaining Pool", db_name), e))
}




// Tests
#[cfg(test)]

mod tests {
    use super::*;
          
    // Ensure the parameters are being correctly combined.
   
    #[test]
    fn check_config_vars_read_correctly() {

        let config = r#"
[data]
data_date="2024-12-11"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
db_name="geo"
"#;

        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            
        let res = get_params(test_args, config_string).unwrap();
    
        assert_eq!(res.flags.import_data, true);
        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.create_config, false);
        assert_eq!(res.flags.initialise, false);
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.data_folder, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.log_folder, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.output_folder, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));
        assert_eq!(res.data_date, "2024-12-11");
    }
    

    #[test]
    fn check_with_i_flag() {

        let config = r#"
[data]
data_date=""

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();
    
        let args : Vec<&str> = vec!["dummy target", "-i", "-x"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        
        let res = get_params(test_args, config_string).unwrap();

        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.create_config, true);
        assert_eq!(res.flags.initialise, true);
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.data_folder, PathBuf::new());
        assert_eq!(res.log_folder, PathBuf::new());
        assert_eq!(res.output_folder, PathBuf::new());
        assert_eq!(res.data_date, "");
    }


    #[test]
    fn check_cli_vars_with_z_flag() {

        let config = r#"
[data]
data_date="2024-12-11"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
db_name="geo"
"#;
        
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();
    
        let args : Vec<&str> = vec!["dummy target", "-z"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
            
        let res = get_params(test_args, config_string).unwrap();
    
        assert_eq!(res.flags.import_data, true);
        assert_eq!(res.flags.export_data, false);
        assert_eq!(res.flags.create_config, false);
        assert_eq!(res.flags.initialise, false);
        assert_eq!(res.flags.test_run, true);
        assert_eq!(res.data_folder, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.log_folder, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.output_folder, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));
        assert_eq!(res.data_date, "2024-12-11");
    }


    #[test]
    #[should_panic]
    fn check_wrong_data_folder_panics() {
    
        let config = r#"
[data]
data_date="2024-12-11"

[files]
data_folder_path="C:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="postgres"
db_password="WinterIsComing!"
db_port="5433"
db_name="geo"
"#;
        
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();
        
        let args : Vec<&str> = vec!["dummy target", "-r"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        
        let _res = get_params(test_args, config_string).unwrap();
    }
    
}

