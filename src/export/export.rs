use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn export_alt_names(_output_folder: &PathBuf, _pool: &Pool<Postgres>) -> Result<(), AppError> {

    Ok(())
}