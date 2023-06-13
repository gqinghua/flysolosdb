use thiserror::Error;

use core::fmt;
use std::{result, fmt::Display};

use sqlparser::parser::ParserError;

///这个类型封装了' std::result '和' SQLRiteError '枚举
///使函数签名更容易阅读。

/// SQLRiteError是一个枚举，包含所有可返回的标准化错误
pub type Result<T> = result::Result<T, SQLRiteError>;
pub type Results<T> = std::result::Result<T, Error>;

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


pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}



impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        match self {
            Error::Config(s) | Error::Internal(s) | Error::Parse(s) | Error::Value(s) => {
                write!(f, "{}", s)
            }
            Error::Abort => write!(f, "Operation aborted"),
            Error::Serialization => write!(f, "Serialization failure, retry transaction"),
            Error::ReadOnly => write!(f, "Read-only transaction"),
        }
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<config::ConfigError> for Error {
    fn from(err: config::ConfigError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<log::ParseLevelError> for Error {
    fn from(err: log::ParseLevelError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<log::SetLoggerError> for Error {
    fn from(err: log::SetLoggerError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Self {
        Error::Value(err.to_string())
    }
}

impl From<rustyline::error::ReadlineError> for Error {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::Parse(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Parse(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::Internal(err.to_string())
    }

}
