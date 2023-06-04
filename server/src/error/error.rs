use thiserror::Error;

use std::result;

use sqlparser::parser::ParserError;

///这个类型封装了' std::result '和' SQLRiteError '枚举
///使函数签名更容易阅读。

/// SQLRiteError是一个枚举，包含所有可返回的标准化错误
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

