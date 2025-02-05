/***************************************************************************
 * Module uses clap crate to read command line arguments. These include 
 * possible A, S, T and C flags, and possible strings for the data folder and 
 * source file name. If no flags 'S' (= import data) is returned by default.
 * Folder and file names return an empty string ("") rather than null if not 
 * present. 
 ***************************************************************************/

use clap::{command, Arg, ArgMatches};
use crate::error_defs::AppError;
use crate::setup::Flags;
use std::ffi::OsString;


pub fn fetch_valid_arguments(args: Vec<OsString>) -> Result<Flags, AppError>
{ 
    let parse_result = parse_args(args)?;

    // Flag values are false if not present, true if present.

    let r_flag = parse_result.get_flag("r_flag");
    let mut x_flag = parse_result.get_flag("x_flag");

    let i_flag = parse_result.get_flag("i_flag");
    let mut j_flag = parse_result.get_flag("j_flag");
    let mut c_flag = parse_result.get_flag("c_flag");

    let z_flag = parse_result.get_flag("z_flag");

    // If c, m, or both flags set (may be by using 'i' (initialise) flag)
    // Only do the c and / or m actions
  
    if i_flag || c_flag || j_flag {

        if i_flag {
            c_flag = true;
            j_flag = true;
        }
        
        Ok(Flags {
            import_data: false,
            export_data: false,
            initialise: c_flag,
            create_config: j_flag,
            test_run: false,
        })
    }
    else {

        if r_flag && x_flag {
            x_flag = false;
        }

        Ok(Flags {
            import_data: r_flag,
            export_data: x_flag,
            create_config: false,
            initialise: false,
            test_run: z_flag,
        })
    }
}


fn parse_args(args: Vec<OsString>) -> Result<ArgMatches, clap::Error> {

    command!()
        .about("Imports data from ROR json file (v2) and imports it into a database")
        .arg(
            Arg::new("r_flag")
           .short('r')
           .long("import")
           .required(false)
           .help("A flag signifying import from ror file to ror schema tables only")
           .action(clap::ArgAction::SetTrue)
        )
       .arg(
             Arg::new("x_flag")
            .short('x')
            .long("filesout")
            .required(false)
            .help("A flag signifying output a summary of the current or specified version into csv files")
            .action(clap::ArgAction::SetTrue)
        )
       .arg(
            Arg::new("i_flag")
           .short('i')
           .long("install")
           .required(false)
           .help("A flag signifying initial run, creates summary and context tables only")
           .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("c_flag")
            .short('c')
            .long("context")
            .required(false)
            .help("A flag signifying that context tables need to be rebuilt")
            .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("j_flag")
            .short('j')
            .long("config")
            .required(false)
            .help("A flag signifying that the conig file should be edited")
            .action(clap::ArgAction::SetTrue)
       )
       .arg(
            Arg::new("z_flag")
            .short('z')
            .long("test")
            .required(false)
            .help("A flag signifying that this is part of an integration test run - suppresses logs")
            .action(clap::ArgAction::SetTrue)
       )
    .try_get_matches_from(args)

}


#[cfg(test)]
mod tests {
    use super::*;
    
    // Ensure the parameters are being correctly extracted from the CLI arguments

    #[test]
    fn check_cli_no_explicit_params() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, true);
        assert_eq!(res.export_data, false);
        assert_eq!(res.initialise, false);
        assert_eq!(res.create_config, false);
        assert_eq!(res.test_run, false);
    }
  
    #[test]
    fn check_cli_with_a_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-a"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, true);
        assert_eq!(res.export_data, true);
        assert_eq!(res.initialise, false);
        assert_eq!(res.create_config, false);
        assert_eq!(res.test_run, false);

    }

    #[test]
    fn check_cli_with_i_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-i"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, false);
        assert_eq!(res.export_data, false);
        assert_eq!(res.initialise, true);
        assert_eq!(res.create_config, true);
        assert_eq!(res.test_run, false);
    }

    #[test]
    fn check_cli_with_c_and_m_flags() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-c", "-m"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, false);
        assert_eq!(res.export_data, false);
        assert_eq!(res.initialise, true);
        assert_eq!(res.create_config, true);
        assert_eq!(res.test_run, false);
    }


    #[test]
    fn check_cli_with_c_and_p_flag() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-c", "-p"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, false);
        assert_eq!(res.export_data, false);
        assert_eq!(res.initialise, true);
        assert_eq!(res.create_config, false);
        assert_eq!(res.test_run, false);
    }

    #[test]
    fn check_cli_with_explicit_string_pars() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-f", "E:\\ROR\\some data folder", 
                                    "-s", "schema2 data.json", "-d", "2025-12-25", "-v", "1.62"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, true);
        assert_eq!(res.export_data, false);
        assert_eq!(res.initialise, false);
        assert_eq!(res.create_config, false);
        assert_eq!(res.test_run, false);
    }

    #[test]
    fn check_cli_with_most_params_explicit() {
        let target = &"target\\debug\\ror1.exe".replace("\\", "/");
        let args : Vec<&str> = vec![target, "-f", "E:\\ROR\\some other data folder", 
        "-s", "schema2.1 data.json", "-d", "2026-12-25", "-v", "1.63", "-r", "-p", "-t", "-z"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();

        let res = fetch_valid_arguments(test_args).unwrap();
        assert_eq!(res.import_data, true);
        assert_eq!(res.export_data, true);
        assert_eq!(res.initialise, false);
        assert_eq!(res.create_config, false);
        assert_eq!(res.test_run, true);
    }

}

