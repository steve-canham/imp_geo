mod export;

use sqlx::{Pool, Postgres};
use crate::AppError;
use std::path::PathBuf;

pub async fn export_data(output_folder: &PathBuf, pool: &Pool<Postgres>) -> Result<(), AppError> {
    
    export::export_alt_names(output_folder, pool).await?;

    Ok(())
}