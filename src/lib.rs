
pub mod setup;
pub mod err;
mod lang_codes;
mod alt_names;
mod cities;
mod countries;
mod admins;
mod scopes;
mod export;


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

    // intiialising routine stuff to go here
        
    if flags.import_data   // import ror from json file and store in ror schema tables
    {
        // The latin_only parameter makes the process include Latin names only
        // By default it is true, but is switchable to false using a command flag.

        let latin_only = !flags.include_nonlatin;

        // Do language codes

        lang_codes::create_lang_code_tables(&pool).await?;
        let file_name = "iso-languagecodes.txt";
        lang_codes::import_data(&params.data_folder, file_name, &pool).await?;
        lang_codes::transfer_data(&pool).await?;

        // Do Alt Names - import first as this data is needed by later imports

        alt_names::create_alt_name_table(&pool).await?;
        let file_name = "alternateNamesV2.txt";
        alt_names::import_data(&params.data_folder, file_name, &pool, latin_only).await?;

        // Admins 1 and 2 data - also used below.

        admins::create_admins_tables(&pool).await?;
        let file_name = "admin1CodesASCII.txt";
        admins::import_data(&params.data_folder, file_name, &pool).await?;
        let file_name = "admin2Codes.txt";
        admins::import_data(&params.data_folder, file_name, &pool).await?;

        // Countries data

        countries::create_country_tables(&pool).await?;
        let file_name = "countryInfo.txt";
        countries::import_data(&params.data_folder, file_name, &pool).await?;

        // Cities data

        cities::create_city_tables(&pool).await?;
        let file_name = "cities5000.txt";
        cities::import_data(&params.data_folder, file_name, &pool).await?;

            // Do 'tidying' of odd and missing values

        // Scope data

        scopes::create_scope_tables(&pool).await?;
        let file_name = "no-country.txt";
        scopes::import_data(&params.data_folder, file_name, &pool, latin_only).await?;
        




        if !test_run {
            //import::summarise_import(&pool).await?;
        }
    }


    if flags.export_data  // write out summary data from data in smm tables
    { 
        export::export_data(&params.output_folder, &pool).await?;
    }


     Ok(())  
}
