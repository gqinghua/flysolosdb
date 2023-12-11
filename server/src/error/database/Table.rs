use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, io};
use thiserror::Error;

use crate::sql::regrxs::types::DataTypesErr;

pub type TableEntries = Vec<HashMap<String, String>>;

pub struct Table<'a> {
    pub db: &'a str,
    pub table_name: &'a str,
}

#[derive(Debug, Error)]
pub enum TableError {
    #[error("DB Error")]
    DBErr(#[from] super::DataError::DatabaseError),
    #[error("IO Error")]
    IoErr(#[from] io::Error),
    #[error("Invalid JSON")]
    SerializationErr(),
    #[error("Table not found")]
    TableNotFond(String),
    #[error("Column not found")]
    ColNotFound(String),
    #[error("Column type  not found")]
    ColTypeNotFound(String),
    #[error("Number of columns doesn't match number of vlaues")]
    NumberMismatch(String),
    #[error("Types errors")]
    TypeErr(#[from] DataTypesErr),
    #[error("Column already exist")]
    ColAlreadyExist(String),
}

pub type TableResult<T> = Result<T, TableError>;
