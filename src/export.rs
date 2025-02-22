use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn export_data(_data_folder: &PathBuf, _source_file_name: &String, _pool: &Pool<Postgres>) -> Result<(), AppError> {

    Ok(())
}