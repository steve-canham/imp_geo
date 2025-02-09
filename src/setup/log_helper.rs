/***************************************************************************
 * Establishes the log for the programme's operation using log and log4rs, 
 * and includes various helper functions.
 * Once established the log file appears to be accessible to any log
 * statement within the rest of the program (after 'use log:: ...').
 ***************************************************************************/

use chrono::Local;
use std::path::PathBuf;
use crate::AppError;
use crate::setup::InitParams;

use log::{info, LevelFilter};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};

pub fn setup_log (data_folder: &PathBuf) -> Result<log4rs::Handle, AppError> {
    let datetime_string = Local::now().format("%m-%d %H%M%S").to_string();
    let log_file_name = format!("geonames data import at {}.log", datetime_string);
    let log_file_path = [data_folder, &PathBuf::from(log_file_name)].iter().collect();
    config_log (&log_file_path)
}


fn config_log (log_file_path: &PathBuf) -> Result<log4rs::Handle, AppError> {
    
    // Initially establish a pattern for each log line.

    let log_pattern = "{d(%d/%m %H:%M:%S)}  {h({l})}  {({M}.{L}):>38.48}:  {m}\n";

    // Define a stderr logger, as one of the 'logging' sinks or 'appender's.

    let stderr = ConsoleAppender::builder().encoder(Box::new(PatternEncoder::new(log_pattern)))
        .target(Target::Stderr).build();

    // Define a second logging sink or 'appender' - to a log file (provided path will place it in the current data folder).

    let try_logfile = FileAppender::builder().encoder(Box::new(PatternEncoder::new(log_pattern)))
        .build(log_file_path);
    let logfile = match try_logfile {
        Ok(lf) => lf,
        Err(e) => return Err(AppError::IoError(e)),
    };

    // Configure and build log4rs instance, using the two appenders described above

    let config = Config::builder()
        .appender(Appender::builder()
                .build("logfile", Box::new(logfile)),)
        .appender(Appender::builder()
                .build("stderr", Box::new(stderr)),)
        .build(Root::builder()
                .appender("logfile")
                .appender("stderr")
                .build(LevelFilter::Info),
        ).unwrap();

    match log4rs::init_config(config)
    {
        Ok(h) => return Ok(h),
        Err(e) => return Err(AppError::LogSetError(e)),
    };

}


pub fn log_startup_params (ip : &InitParams) {
    
    // Called at the end of set up to record the input parameters

    info!("PROGRAM START");
    info!("");
    info!("************************************");
    info!("");
    info!("data_folder: {}", ip.data_folder.display());
    info!("log_folder: {}", ip.log_folder.display());
    info!("output_folder: {}", ip.output_folder.display());
    info!("data_date: {}", ip.data_date);
    info!("initialising: {}", ip.flags.initialise);
    info!("create config: {}", ip.flags.create_config);
    info!("import_ror: {}", ip.flags.import_data);
    info!("export_text: {}", ip.flags.export_data);
    info!("");
    info!("************************************");
    info!("");
}