use thiserror::Error;

use core::fmt;
use std::{result, fmt::Display};

use sqlparser::parser::ParserError;

///这个类型封装了' std::result '和' SQLRiteError '枚举
///使函数签名更容易阅读。

/// SQLRiteError是一个枚举，包含所有可返回的标准化错误
pub type Result<T> = result::Result<T, SQLRiteError>;
pub type Results<T> = std::result::Result<T, Errors>;

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


pub enum Errors {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}



impl Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            Errors::Config(s) | Errors::Internal(s) | Errors::Parse(s) | Errors::Value(s) => {
                write!(f, "{}", s)
            }
            Errors::Abort => write!(f, "Operation aborted"),
            Errors::Serialization => write!(f, "Serialization failure, retry transaction"),
            Errors::ReadOnly => write!(f, "Read-only transaction"),
        }
    }
}

impl From<Box<bincode::ErrorKind>> for Errors {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl From<config::ConfigError> for Errors {
    fn from(err: config::ConfigError) -> Self {
        Errors::Config(err.to_string())
    }
}

impl From<log::ParseLevelError> for Errors {
    fn from(err: log::ParseLevelError) -> Self {
        Errors::Config(err.to_string())
    }
}

impl From<log::SetLoggerError> for Errors {
    fn from(err: log::SetLoggerError) -> Self {
        Errors::Config(err.to_string())
    }
}

impl From<regex::Error> for Errors {
    fn from(err: regex::Error) -> Self {
        Errors::Value(err.to_string())
    }
}

impl From<rustyline::error::ReadlineError> for Errors {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Errors {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl From<std::io::Error> for Errors {
    fn from(err: std::io::Error) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl From<std::net::AddrParseError> for Errors {
    fn from(err: std::net::AddrParseError) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Errors {
    fn from(err: std::num::ParseFloatError) -> Self {
        Errors::Parse(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Errors {
    fn from(err: std::num::ParseIntError) -> Self {
        Errors::Parse(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Errors {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Errors::Internal(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Errors {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Errors::Internal(err.to_string())
    }

}
