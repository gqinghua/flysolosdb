use std::num::ParseIntError;

use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::regexs::{RE_ENUM, RE_ENUM_VALUES, RE_VARCHAR};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DataTypesErr {
    #[error("Invalid Type")]
    InvalidType(String),
    #[error("Invalid varchar Type")]
    InvalidVarchar(#[from] ParseIntError),
    #[error("Invalid number")]
    InvalidInt(String),
    #[error("Invalid float")]
    InvalidFloat(String),
    #[error("Invalid float")]
    InvalidEnum(String),
    #[error("Invalid boolean")]
    InvalidBool(String),
    #[error("Invalid string")]
    InvalidStr(String),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum DataType {
    // Numeric datatypes
    INTEGER,
    INT,
    FLOAT,
    DEC,
    // String datatypes
    TEXT,
    VARCHAR(usize),
    ENUM(Vec<String>),
    BOOLEAN,
    BOOL,
}

impl DataType {
    pub fn parse(datatype: &str) -> Result<Self, DataTypesErr> {
        let re_varchar = Regex::new(RE_VARCHAR).unwrap();
        let re_enum = Regex::new(RE_ENUM).unwrap();
        let re_enum_values = Regex::new(RE_ENUM_VALUES).unwrap();
        let dt = datatype.trim();

        if let Some(caps) = re_varchar.captures(dt) {
            let size = match caps.name("size") {
                Some(_) => match caps["size"].parse::<usize>() {
                    Ok(s) => s,
                    Err(e) => return Err(DataTypesErr::InvalidVarchar(e)),
                },
                None => 255,
            };

            return Ok(DataType::VARCHAR(size));
        }

        if let Some(caps) = re_enum.captures(dt) {
            let values = re_enum_values
                .captures_iter(&caps["values"])
                .map(|caps| caps["value"].trim().to_string())
                .filter(|v| !v.is_empty())
                .collect::<Vec<_>>();
            return Ok(DataType::ENUM(values));
        }

        let dt = dt.to_uppercase();
        let dt = match dt {
            _ if DataType::INTEGER.as_string() == dt => DataType::INTEGER,
            _ if DataType::INT.as_string() == dt => DataType::INT,
            _ if DataType::FLOAT.as_string() == dt => DataType::FLOAT,
            _ if DataType::DEC.as_string() == dt => DataType::DEC,
            _ if DataType::TEXT.as_string() == dt => DataType::TEXT,
            _ if DataType::BOOLEAN.as_string() == dt => DataType::BOOLEAN,
            _ if DataType::BOOL.as_string() == dt => DataType::BOOL,

            _ => return Err(DataTypesErr::InvalidType(datatype.trim().into())),
        };

        return Ok(dt);
    }

    pub fn as_string(&self) -> String {
        format!("{:?}", self)
    }

    pub fn is_valid(&self, raw: &str) -> Result<(), DataTypesErr> {
        return match self {
            DataType::INTEGER | DataType::INT if raw.parse::<i64>().is_err() => Err(
                DataTypesErr::InvalidInt(format!("'{}' is not a valid {:?}", raw, self)),
            ),
            DataType::FLOAT | DataType::DEC if raw.parse::<f64>().is_err() => Err(
                DataTypesErr::InvalidFloat(format!("'{}' is not a valid {:?}", raw, self)),
            ),
            DataType::VARCHAR(max_len) if &raw.len() > max_len => Err(DataTypesErr::InvalidStr(
                format!("Max length exceed of `{}`. Max len = {}", raw, max_len),
            )),
            DataType::ENUM(values) if values.iter().position(|v| v == raw).is_none() => {
                Err(DataTypesErr::InvalidEnum(format!(
                    "`{}` is not valid enum. must be one of these {:?}",
                    raw, values
                )))
            }
            DataType::BOOLEAN | DataType::BOOL if raw.parse::<bool>().is_err() => Err(
                DataTypesErr::InvalidBool(format!("`{}` is not a valid boolean", raw)),
            ),
            _ => Ok(()),
        };
    }

    pub fn default(&self) -> String {
        let res = match self {
            DataType::INTEGER | DataType::INT => "0",
            DataType::FLOAT | DataType::DEC => "0.0",
            DataType::TEXT | DataType::VARCHAR(_) => "",
            DataType::ENUM(val) => val[0].as_str(),
            DataType::BOOLEAN | DataType::BOOL => "false",
        };

        res.to_string()
    }
}

