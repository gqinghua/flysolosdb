use thiserror::Error;

use std::result;

use sqlparser::parser::ParserError;

/// This is a type that encapsulated the `std::result` with the enum `SQLRiteError`
/// and makes function signatures easier to read.

/// SQLRiteError is an enum with all the standardized errors available for returning
pub type Result<T> = result::Result<T, SQLRiteError>;
///
#[derive(Error, Debug, PartialEq)]
pub enum SQLRiteError {
    #[error("Not Implemented error: {0}")]
    NotImplemented(String),
    #[error("General error: {0}")]
    General(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Unknown command error: {0}")]
    UnknownCommand(String),
    #[error("SQL error: {0:?}")]
    SqlError(#[from] ParserError),
}

/// Returns SQLRiteError::General error from String
pub fn sqlrite_error(message: &str) -> SQLRiteError {
    SQLRiteError::General(message.to_owned())
}

