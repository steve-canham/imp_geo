pub mod setup;

use thiserror::Error;
use setup::log_helper;
use std::ffi::OsString;
use std::path::PathBuf;
use std::fs;

pub async fn run(args: Vec<OsString>) -> Result<(), AppError> {
    
    // Important that there are no errors in the intial three steps.
    // If one does occur the program exits.
    // 1) Collect initial parameters such as file names and CLI flags. 
    // CLI arguments are collected explicitly to facilitate unit testing. 
    // of 'get_params'. Relevant environmental variables are also read.
    // 2) Establish a log file, in the specified data folder.
    // The initial parameters are recorded as the initial part of the log.
    // 3) The database connection pool is established for the database "ror".

    let config_file = PathBuf::from("./xxapp_config.toml");
    let config_string: String = fs::read_to_string(&config_file)
                                .map_err(|e| AppError::IoReadErrorWithPath(e, config_file))?;
                              
    let params = setup::get_params(args, config_string).await?;
    let flags = params.flags;
    let test_run = flags.test_run;

    if !test_run {
       log_helper::setup_log(&params.log_folder)?;
       log_helper::log_startup_params(&params);
    }
            
    let _pool = setup::get_db_pool().await?; 

    Ok(())
}




// The error types used within the program.

#[derive(Error, Debug)]
pub enum AppError {

    #[error("Error when processing command line arguments: {0:?}")]
    ClapError(#[from] clap::Error),

    #[error("Error when processing sql: {0:?}")]
    SqlxError(#[from] sqlx::Error),

    #[error("Error during IO operation: {0:?}")]
    IoError(#[from] std::io::Error),

    #[error("couldn't read file {1:?}")]
    IoReadErrorWithPath(#[source] std::io::Error, std::path::PathBuf,),

    #[error("JSON processing error: {0:?}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Error when setting up log configuration: {0:?}")]
    LogSetError(#[from] log::SetLoggerError),

    #[error("Database Parameter Unavailable: {0:?} ")]
    MissingDatabaseParameter(String),

    #[error("CRITICAL Config Error: {0:?}")]
    CriticalConfigError(String),

    #[error("The folder '{0}' is required, but has not been supplied or is not accessible")]
    MissingFolder(String),

    #[error("The parameter '{0}' is required, but has not been supplied")]
    MissingParameter(String),

}

