use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Duplicated database")]
    DuplicatedDB(String),
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Database not found")]
    NotFound(String),
}

type DBResult<T> = Result<T, DatabaseError>;
