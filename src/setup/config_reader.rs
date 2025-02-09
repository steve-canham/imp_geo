/***************************************************************************
 * 
 ***************************************************************************/

use std::sync::OnceLock;
use toml;
use serde::Deserialize;
use std::path::PathBuf;
use crate::AppError;

pub static DB_PARS: OnceLock<DBPars> = OnceLock::new();

#[derive(Debug, Deserialize)]
pub struct TomlConfig {
    pub data: Option<TomlDataPars>,
    pub files: Option<TomlFilePars>, 
    pub database: Option<TomlDBPars>,
}

#[derive(Debug, Deserialize)]
pub struct TomlDataPars {
    pub data_date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TomlFilePars {
    pub data_folder_path: Option<String>,
    pub log_folder_path: Option<String>,
    pub output_folder_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TomlDBPars {
    pub db_host: Option<String>,
    pub db_user: Option<String>,
    pub db_password: Option<String>,
    pub db_port: Option<String>,
    pub db_name: Option<String>,
}

pub struct Config {
    pub data_details: DataPars, 
    pub files: FilePars, 
    pub db_pars: DBPars,
}

pub struct DataPars {
    pub data_date: String,
}

pub struct FilePars {
    pub data_folder_path: PathBuf,
    pub log_folder_path: PathBuf,
    pub output_folder_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DBPars {
    pub db_host: String,
    pub db_user: String,
    pub db_password: String,
    pub db_port: usize,
    pub db_name: String,
}


pub fn populate_config_vars(config_string: &String) -> Result<Config, AppError> {

    let toml_config = toml::from_str::<TomlConfig>(&config_string)
        .map_err(|_| {
            let problem = r#"Unable to open config file - the program cannot continue. 
            Please check config file ('app_config.toml') has the 
            correct name, form and is in the correct location ."#.to_string(); 
            AppError::CriticalConfigError(problem)
        }
    )?;

    let toml_data_details = match toml_config.data {
        Some(d) => d,
        None => {
            println!("Data detals section not found in config file.");
            TomlDataPars {
                data_date: None,  // Legitimate in some circumstances
            }
        },
    };

    let toml_database = match toml_config.database {
        Some(d) => d,
        None => {
            let problem = r#"Unable to read any DB parameters - the program cannot continue. 
            Please check the config file has a set of appropriate values listed
            under '[database]'"#.to_string(); 
            return Result::Err(AppError::CriticalConfigError(problem))  
        },
    };

    let toml_files = match toml_config.files {
        Some(f) => f,
        None => {
            let problem = r#"Unable to read any file parameters - the program cannot continue. 
            Please check the config file has a set of appropriate values listed
            under '[files]'"#.to_string();  
            return Result::Err(AppError::CriticalConfigError(problem))  
        },
    };
   
    let config_files = verify_file_parameters(toml_files)?;
    let config_data_dets = verify_data_parameters(toml_data_details);
    let config_db_pars = verify_db_parameters(toml_database)?;

    let _ = DB_PARS.set(config_db_pars.clone()); 

    Ok(Config{
        data_details: config_data_dets,
        files: config_files,
        db_pars: config_db_pars,
    })
}


fn verify_data_parameters(toml_data_pars: TomlDataPars) -> DataPars {

    let data_date = match toml_data_pars.data_date {
        Some(s) => s,
        None => "".to_string(),
    };

    DataPars {
        data_date,
    }
}


fn verify_file_parameters(toml_files: TomlFilePars) -> Result<FilePars, AppError> {

    // Check data folder first as there are no default for this.
    // It must therefore be present.

    let data_folder_path = check_critical_pathbuf (toml_files.data_folder_path, 
                            "Unable to read the data folder path in config file.",
                            "has a value for the data_folder_path",
               )?;

    let log_folder_path = check_pathbuf (toml_files.log_folder_path, "log folder", &data_folder_path);

    let output_folder_path = check_pathbuf (toml_files.output_folder_path, "outputs folder", &data_folder_path);

    Ok(FilePars {
        data_folder_path,
        log_folder_path,
        output_folder_path,
    })
}


fn check_critical_pathbuf (src_name: Option<String>, problem: &str, action: &str) -> Result<PathBuf, AppError> {
 
    let s = match src_name {
        Some(s) => s,
        None => "none".to_string(),
    };

    if s == "none".to_string() || s.trim() == "".to_string()
    {
        let err_msg = problem.to_string() + r#"
        The program cannot continue. 
        Please check the config file ('app_config.toml') "# + action;
        Err(AppError::CriticalConfigError(err_msg))
    }
    else {
        Ok(PathBuf::from(s))
    }
}


fn check_pathbuf (src_name: Option<String>, folder_type: &str, alt_path: &PathBuf) -> PathBuf {
 
    let s = match src_name {
        Some(s) => s,
        None => "none".to_string(),
    };

    if s == "none".to_string() || s.trim() == "".to_string()
    {
        let print_msg = r#"No value found for "#.to_string() + folder_type + r#" path in config file - 
            using the provided data folder instead."#;
        println!("{}", print_msg);
        alt_path.to_owned()
    }
    else {
        PathBuf::from(s)
    }
}


fn verify_db_parameters(toml_database: TomlDBPars) -> Result<DBPars, AppError> {

 // Check user name and password first as there are no defaults for these values.
    // They must therefore be present.

    let db_user = check_critical_db_par (toml_database.db_user , 
        "Unable to read the user name from the config file.", "has a value for db_user")?; 

    let db_password = check_critical_db_par (toml_database.db_password , 
        "Unable to read the user password from the config file.", "has a value for db_password")?; 

    let db_host = check_db_par (toml_database.db_host, "DB host", "localhost");
            
    let db_port_as_string = check_db_par (toml_database.db_port, "DB port", "5432");
    let db_port: usize = db_port_as_string.parse().unwrap_or_else(|_| 5432);

    let db_name = check_db_par (toml_database.db_name, "DB name", "geo");

    Ok(DBPars {
        db_host,
        db_user,
        db_password,
        db_port,
        db_name,
    })
}


fn check_critical_db_par (src_name: Option<String>, problem: &str, action: &str) -> Result<String, AppError> {
 
    let s = match src_name {
        Some(s) => s,
        None => "none".to_string(),
    };

    if s == "none".to_string() || s.trim() == "".to_string()
    {
        let err_msg = problem.to_string() + r#" 
        The program cannot continue. 
        Please check the config file ('app_config.toml') "# + action;
        Err(AppError::CriticalConfigError(err_msg))
    }
    else {
        Ok(s)
    }
}


fn check_db_par (src_name: Option<String>, folder_type: &str, default:  &str) -> String {
 
    let s = match src_name {
        Some(s) => s,
        None => "none".to_string(),
    };

    if s == "none".to_string() || s.trim() == "".to_string()
    {
        let print_msg = r#"No value found for "#.to_string() + folder_type + r#" path in config file - 
            using the provided default value instead."#;
        println!("{}", print_msg);
        default.to_owned()
    }
    else {
       s
    }
}


pub fn fetch_db_name() -> Result<String, AppError> {
    let db_pars = match DB_PARS.get() {
         Some(dbp) => dbp,
         None => {
            let problem = "Unable to obtain DB name.".to_string();
            return Result::Err(AppError::MissingDatabaseParameter(problem));
        },
    };
    Ok(db_pars.db_name.clone())
}


pub fn fetch_db_conn_string(db_name: String) -> Result<String, AppError> {
    let db_pars = match DB_PARS.get() {
         Some(dbp) => dbp,
         None => {
            let problem = "Unable to obtain DB parameters when building connection string.".to_string();
            return Result::Err(AppError::MissingDatabaseParameter(problem));
        },
    };
    
    Ok(format!("postgres://{}:{}@{}:{}/{}", 
    db_pars.db_user, db_pars.db_password, db_pars.db_host, db_pars.db_port, db_name))
}


#[cfg(test)]
mod tests {
    use super::*;
    
    // Ensure the parameters are being correctly extracted from the config file string
    
    #[test]
    fn check_config_with_all_params_present() {

        let config = r#"
[data]
data_date="2026-06-15"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();
        assert_eq!(res.files.data_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.log_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.files.output_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));

        assert_eq!(res.data_details.data_date, "2026-06-15");

        assert_eq!(res.db_pars.db_host, "localhost");
        assert_eq!(res.db_pars.db_user, "user_name");
        assert_eq!(res.db_pars.db_password, "password");
        assert_eq!(res.db_pars.db_port, 5433);
        assert_eq!(res.db_pars.db_name, "geo");
    }


    #[test]
    fn check_config_with_missing_log_and_outputs_folders() {

        let config = r#"
[data]
data_date="2026-06-15"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();
        assert_eq!(res.files.data_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.log_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.output_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
    }


    #[test]
    fn check_config_with_blank_log_and_outputs_folders() {

        let config = r#"
[data]
data_date="2026-06-15"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path=""
output_folder_path=""

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();
        assert_eq!(res.files.data_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.log_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.output_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
    }


    #[test]
    fn check_missing_data_details_become_empty_strings() {

        let config = r#"
[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();
        assert_eq!(res.files.data_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\data"));
        assert_eq!(res.files.log_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\logs"));
        assert_eq!(res.files.output_folder_path, PathBuf::from("E:\\MDR source data\\Geonames\\outputs"));

        assert_eq!(res.data_details.data_date, "");

        assert_eq!(res.db_pars.db_host, "localhost");
        assert_eq!(res.db_pars.db_user, "user_name");
        assert_eq!(res.db_pars.db_password, "password");
        assert_eq!(res.db_pars.db_port, 5433);
        assert_eq!(res.db_pars.db_name, "geo");
    }


    #[test]
    #[should_panic]
    fn check_missing_data_folder_panics() {
    let config = r#"
[data]
data_date="2026-06-15"

[files]
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let _res = populate_config_vars(&config_string).unwrap();
    }


    #[test]
    #[should_panic]
    fn check_missing_user_name_panics() {

        let config = r#"
[data]
data_date="2026-06-15"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user=""
db_password="password"
db_port="5433"
db_name="geo"
"#;
        let config_string = config.to_string();
        let _res = populate_config_vars(&config_string).unwrap();
    }


    #[test]
    fn check_db_defaults_are_supplied() {

        let config = r#"
[data]
data_date="2026-06-15"

[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_user="user_name"
db_password="password"
"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();
        assert_eq!(res.db_pars.db_host, "localhost");
        assert_eq!(res.db_pars.db_user, "user_name");
        assert_eq!(res.db_pars.db_password, "password");
        assert_eq!(res.db_pars.db_port, 5432);
        assert_eq!(res.db_pars.db_name, "geo");
    }


#[test]
    fn missing_port_gets_default() {

        let config = r#"
[files]
data_folder_path="E:\\MDR source data\\Geonames\\data"
log_folder_path="E:\\MDR source data\\Geonames\\logs"
output_folder_path="E:\\MDR source data\\Geonames\\outputs"

[database]
db_host="localhost"
db_user="user_name"
db_password="password"
db_port=""
db_name="geo"

"#;
        let config_string = config.to_string();
        let res = populate_config_vars(&config_string).unwrap();

        assert_eq!(res.data_details.data_date, "");

        assert_eq!(res.db_pars.db_host, "localhost");
        assert_eq!(res.db_pars.db_user, "user_name");
        assert_eq!(res.db_pars.db_password, "password");
        assert_eq!(res.db_pars.db_port, 5432);
        assert_eq!(res.db_pars.db_name, "geo");
    }

}
  



