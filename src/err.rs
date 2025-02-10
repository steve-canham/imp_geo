use thiserror::Error;

// The error types used within the program.

#[derive(Error, Debug)]
pub enum AppError {

    #[error("Error in configuration file: {0:?} {1:?} ")]
    ConfigurationError(String, String),

    #[error("Database Parameters Unavailable")]
    MissingDBParameters(),

    #[error("The parameter '{0}' is required, but has not been supplied")]
    MissingProgramParameter(String),

    #[error("couldn't read file {1:?}")]
    IoReadErrorWithPath(#[source] std::io::Error, std::path::PathBuf,),

    #[error("couldn't write file {1:?}")]
    IoWriteErrorWithPath(#[source] std::io::Error, std::path::PathBuf,),

    #[error("Error when setting up log configuration: {0:?} {1:?}")]
    LogSetupError(String, String),

    #[error("Error when processing command line arguments: {0:?}")]
    ClapError(#[from] clap::Error),

    #[error("JSON processing error: {0:?}")]
    SerdeError(#[from] serde_json::Error),

    #[error("Error when creating a DB Pool: {0:?}")]
    DBPoolError(#[source] sqlx::Error, String,),

    #[error("Error when processing sql: {0:?}")]
    SqlxError(#[from] sqlx::Error),

    #[error("Error during IO operation: {0:?}")]
    IoError(#[from] std::io::Error),
}


pub fn report_error(e: AppError) -> () {

    match e {

        AppError::ConfigurationError(p, d) => print_error (p, d, "CONFIGURATION ERROR"),

        AppError::ClapError(e) => print_error ("Error occureed when parsing CLI argumants".to_string(), 
                    e.to_string(), "CLAP ERROR"),

         AppError::MissingDBParameters() => print_error ("Unable to obtain database parameters.".to_string(),
                    "Attempting to read OnceLock<DB_PARS>".to_string(), "DB PARAMETERS ERROR"),            

        AppError::MissingProgramParameter(p) =>  print_error (
                  "A required parameter is neither in the config file nor the command line arguments".to_string(), 
                  format!("Parameter is: {}", p), "MISSING PARAMETER"),

        AppError::LogSetupError(p, d) => print_error (p, d, "LOG SETUP ERROR"),

        AppError::IoReadErrorWithPath(e, p) => print_error (e.to_string(), 
                  "Path was: ".to_string() + p.to_str().unwrap(), "FILE READING PROBLEM"),
        
        AppError::IoWriteErrorWithPath(e, p) => print_error (e.to_string(), 
                  "Path was: ".to_string() + p.to_str().unwrap(), "FILE WRITING PROBLEM"),
        
        AppError::SerdeError(e) => print_error ("Error occureed when parsing JSON file".to_string(), 
                    e.to_string(), "SERDE JSON ERROR"),
        
        AppError::DBPoolError(e, n) => print_error(format!("An error occured while creating the DB pool: {}", e), 
                   format!("Database requested was: {}", n), "DB POOL ERROR"),
  
        AppError::SqlxError(e) => print_simple_error (e.to_string(), "SQLX ERROR"),
  
        AppError::IoError(e) => print_simple_error (e.to_string(), "IO ERROR"),

    }

    //process::exit(1);
}


fn print_simple_error(msg: String, header: &str) {
    eprintln!("");
    let star_num = 100;
    let hdr_line = get_header_line (star_num, &header);
    let starred_line = str::repeat("*", star_num);

    eprintln!("{}", hdr_line);
    let lines =  msg.split(".");
    for l in lines {
        eprintln!("{}.", l.trim());
    }
    eprintln!("{}", starred_line);
    eprintln!("");
}


fn print_error(description: String, details: String, header: &str) {
    eprintln!("");
    let star_num = 100;
    let hdr_line = get_header_line (star_num, &header);
    let starred_line = str::repeat("*", star_num);
    
    eprintln!("{}", hdr_line);
    eprintln!("{}", description);
    eprintln!("{}", details);
    eprintln!("{}", starred_line); 
    eprintln!("");
}


fn get_header_line (star_num: usize, header: &str) -> String {
    let hdr_len = header.len();
    let mut spacer = "";
    if hdr_len % 2 != 0  {
        spacer = " ";
    }
    let star_batch_num = (star_num - 2 - hdr_len) / 2;
    let star_batch = str::repeat("*", star_batch_num);
    format!("{} {}{} {}", star_batch, header, spacer, star_batch)
}



