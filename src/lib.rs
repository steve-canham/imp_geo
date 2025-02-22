
pub mod setup;
pub mod err;
mod initialise;
mod import;
mod export;
mod data_vectors;

use setup::cli_reader;
use err::AppError;
use std::ffi::OsString;
use std::path::PathBuf;
use std::fs;

pub async fn run(args: Vec<OsString>) -> Result<(), AppError> {

    let cli_pars: cli_reader::CliPars;
    cli_pars = cli_reader::fetch_valid_arguments(args)?;
    let flags = cli_pars.flags;

    let config_file = PathBuf::from("./app_config.toml");
    let config_string: String = fs::read_to_string(&config_file)
                                .map_err(|e| AppError::IoReadErrorWithPath(e, config_file))?;
                              
    let params = setup::get_params(cli_pars, &config_string)?;
    setup::establish_log(&params)?;
    let pool = setup::get_db_pool().await?;
    let test_run = flags.test_run;
        
    if flags.import_data   // import ror from json file and store in ror schema tables
    {
        initialise::create_geo_tables(&pool).await?;

        // The fourth parameter, true, makes the process include Latin names only
        // By default it is true, but needs to be switchable to false using a command flag
        let latin_only = !flags.include_nonlatin;
        import::import_data(&params.data_folder, &params.source_file_name, &pool, latin_only).await?;

        if !test_run {
            //import::summarise_import(&pool).await?;
        }
    }

    if flags.export_data  // write out summary data from data in smm tables
    { 
        export::export_data(&params.output_folder, &params.source_file_name, &pool).await?;
    }


     Ok(())  
}
