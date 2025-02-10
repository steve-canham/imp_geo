
use imp_geo::AppError;
use imp_geo::run;
use std::env;

#[tokio::main(flavor = "current_thread")]
async fn main() ->  () {

    let args: Vec<_> = env::args_os().collect();
    match run(args).await
    {
      Ok(_) => println!("Done!"),
      Err(e) => report_error(e),
    };

    ()
}


fn report_error(e: AppError) -> () {

    match e {

        AppError::CriticalConfigError(e) =>  print_error (e, "CONFIG FILE ERROR"),

        AppError::MissingDatabaseParameter(e) =>  print_error (e, "MISSING DATABASE PARAMETER"),

        AppError::MissingFolder(e) =>  print_error (e, "MISSING FOLDER"),

        AppError::MissingParameter(e) =>  print_error (e, "MISSING PARAMETER"),

        AppError::IoReadErrorWithPath(_e, p) => print_error (p.to_str().unwrap().to_string(), "FILE READING PROBLEM"),
        
        
        _ => print_error (e.to_string(), "ERROR ERROR ERROR"),
    }

    //process::exit(1);
}


fn print_error(msg: String, header: &str) {
    eprintln!("");
    let star_num = 100;
    let starred_line = str::repeat("*", star_num);
    let hdr_len = header.len();
    let mut spacer = "";
    if hdr_len % 2 != 0  {
        spacer = " ";
    }
    let star_batch_num = (star_num - 2 - hdr_len) / 2;
    let star_batch = str::repeat("*", star_batch_num);
    eprintln!("{} {}{} {}", star_batch, header, spacer, star_batch);
    let lines =  msg.split(".");
    for l in lines {
        eprintln!("{}.", l.trim());
    }
    eprintln!("{}", starred_line);
}



/*
    match e {

        ClapError => println!("{:?}", e),
        
        SqlxError =>  println!("{:?}", &e),
        
        IoError =>  println!("{:?}", &e),

        SerdeError =>  println!("{:?}", &e),

        LogSetError =>  println!("{:?}", &e),

        MissingDatabaseParameter =>  println!("{:?}", &e),

        CriticalConfigError =>  println!("{:?}", &e),

        MissingFolder =>  println!("{:?}", &e),

        MissingParameter =>  println!("{:?}", &e),

      
        #[error("Error when processing command line arguments: {0:?}")]
        ClapError(#[from] clap::Error),
    
        #[error("Error when processing sql: {0:?}")]
        SqlxError(#[from] sqlx::Error),
    
        #[error("Error during IO operation: {0:?}")]
        IoError(#[from] std::io::Error),
    
        #[error("JSON processing error: {0:?}")]
        SerdeError(#[from] serde_json::Error),
    
        #[error("Error when setting up log configuration: {0:?}")]
        LogSetError(#[from] log::SetLoggerError),
    
        #[error("Database Parameter Unavailable: {0:?} ")]
        MissingDatabaseParameter(String),
    
        #[error("CRITICAL Config Error - {0:?}")]
        CriticalConfigError(String),
    
        #[error("The folder '{0}' is required, but has not been supplied or is not accessible")]
        MissingFolder(String),
    
        #[error("The parameter '{0}' is required, but has not been supplied")]
        MissingParameter(String),
    
    }
 
    */
    

 
