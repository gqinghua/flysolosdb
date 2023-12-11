use regex::Regex;
use thiserror::Error;

use crate::sql::regrxs::{
    regexs::{
        RE_ADD_COL, RE_ALTER_COL, RE_CREATE_TABLE, RE_DB, RE_DELETE_FROM_TABLE, RE_DROP_COL,
        RE_INSERT, RE_INSERT_VALUES_VALUES, RE_KEY_VALUE, RE_SELECT, RE_SHOW_QUERY, RE_TABLE,
        RE_TABLE_ENTRIES,
    },
    *,
};
use types::{DataType, DataTypesErr};
use utils::{get_cols, get_comma_separated_values};
pub type ColName = String;

#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseAction {
    Create,
    Drop,
    Use,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TableQuery {
    Create {
        cols: Vec<String>,
        types: Vec<DataType>,
    },
    DropTable,
    Truncate,
    AddCol {
        col_name: String,
        datatype: DataType,
    },
    AlterCol {
        col_name: String,
        datatype: DataType,
    },
    DropCol(ColName),
    Select {
        cols: SelectCols,
        condition: Option<Condition>,
    },
    Insert {
        cols: SelectCols,
        values: Vec<Vec<String>>,
    },
    Delete {
        condition: Condition,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum SelectCols {
    All,
    Cols(Vec<String>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Query {
    ShowAllDBs,
    ShowCurrDB,
    ShowTables,
    Database {
        name: String,
        action: DatabaseAction,
    },
    Table {
        name: String,
        query: TableQuery,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum QueryParserError {
    #[error("Failed to parse the query")]
    BadQuery(String),
    #[error("Invalid DB query")]
    InvalidDBAction(String),
    #[error("Invalid query")]
    InvalidTableAction(String),
    #[error("Invalid condition")]
    InvalidCondition(String),
    #[error("Invalid Operator")]
    InvalidOperator(String),
    #[error("Data type errors")]
    DataTypeErr(#[from] DataTypesErr),
}

pub struct QueryParser;
impl QueryParser {
    pub fn parse(mut query: &str) -> Result<Query, QueryParserError> {
        query = query.trim();
        let re_show = Regex::new(RE_SHOW_QUERY).unwrap();

        if let Some(caps) = re_show.captures(query) {
            return match caps["query"].to_lowercase().as_str() {
                "databases" => Ok(Query::ShowAllDBs),
                "current database" => Ok(Query::ShowCurrDB),
                "tables" => Ok(Query::ShowTables),
                _ => Err(QueryParserError::BadQuery(query.to_string())),
            };
        }

        let re_db = Regex::new(RE_DB).unwrap();
        if let Some(caps) = re_db.captures(query) {
            let name = caps["name"].to_string();
            let action = &caps["action"];

            let action = match action.to_lowercase().as_str() {
                "create" => DatabaseAction::Create,
                "drop" => DatabaseAction::Drop,
                "use" => DatabaseAction::Use,
                _ => return Err(QueryParserError::InvalidDBAction(action.to_string())),
            };

            return Ok(Query::Database { name, action });
        }

        let re_create_table = Regex::new(RE_CREATE_TABLE).unwrap();
        if let Some(caps) = re_create_table.captures(query) {
            let table_name = caps["name"].to_string();
            let re_entries = Regex::new(RE_TABLE_ENTRIES).unwrap();
            let mut types = Vec::new();
            let mut cols = Vec::new();
            for caps in re_entries.captures_iter(&caps["entries"]) {
                types.push(DataType::parse(&caps["col_type"])?);
                cols.push(caps["col_name"].to_string())
            }

            return Ok(Query::Table {
                name: table_name,
                query: TableQuery::Create { cols, types },
            });
        }

        let re_table = Regex::new(RE_TABLE).unwrap();

        if let Some(caps) = re_table.captures(query) {
            let table_name = caps["name"].to_string();
            match caps["action"].to_lowercase().as_str() {
                "drop" => {
                    return Ok(Query::Table {
                        name: table_name,
                        query: TableQuery::DropTable,
                    })
                }
                "truncate" => {
                    return Ok(Query::Table {
                        name: table_name,
                        query: TableQuery::Truncate,
                    })
                }
                _ => {
                    return Err(QueryParserError::InvalidTableAction(
                        caps["action"].to_string(),
                    ))
                }
            };
        }

        let re_drop_col = Regex::new(RE_DROP_COL).unwrap();
        if let Some(caps) = re_drop_col.captures(query) {
            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::DropCol(caps["col_name"].to_string()),
            });
        }

        let re_alter_col = Regex::new(RE_ALTER_COL).unwrap();
        if let Some(caps) = re_alter_col.captures(query) {
            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::AlterCol {
                    col_name: caps["col_name"].to_string(),
                    datatype: DataType::parse(&caps["datatype"])?,
                },
            });
        }

        let re_add_col = Regex::new(RE_ADD_COL).unwrap();
        if let Some(caps) = re_add_col.captures(query) {
            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::AddCol {
                    col_name: caps["col_name"].to_string(),
                    datatype: DataType::parse(&caps["datatype"])?,
                },
            });
        }

        let re_select = Regex::new(RE_SELECT).unwrap();
        if let Some(caps) = re_select.captures(query) {
            let condition = caps.name("condition").map(|_| &caps["condition"]);

            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::Select {
                    condition: match condition {
                        None => None,
                        Some(c) => Some(Condition::parse(c)?),
                    },
                    cols: get_cols(&caps["cols"]),
                },
            });
        }

        let re_insert = Regex::new(RE_INSERT).unwrap();
        if let Some(caps) = re_insert.captures(query) {
            let cols = match caps.name("cols") {
                Some(_) => SelectCols::Cols(get_comma_separated_values(&caps["cols"])),
                None => SelectCols::All,
            };

            let re_values = Regex::new(RE_INSERT_VALUES_VALUES).unwrap();
            let values = re_values
                .captures_iter(&caps["values"])
                .map(|caps| get_comma_separated_values(&caps["row"]))
                .collect::<Vec<Vec<_>>>();

            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::Insert { cols, values },
            });
        }

        let re_delete = Regex::new(RE_DELETE_FROM_TABLE).unwrap();
        if let Some(caps) = re_delete.captures(query) {
            let condition = Condition::parse(&caps["condition"])?;
            return Ok(Query::Table {
                name: caps["table_name"].to_string(),
                query: TableQuery::Delete { condition },
            });
        }

        Err(QueryParserError::BadQuery(query.to_string()))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Condition {
    pub key: String,
    pub value: String,
    pub operator: Operator,
}

impl Condition {
    fn parse(query: &str) -> Result<Condition, QueryParserError> {
        let re = Regex::new(RE_KEY_VALUE).unwrap();

        match re.captures(query) {
            Some(caps) => {
                let operator = match &caps["operator"] {
                    "=" => Operator::Eq,
                    "!=" => Operator::NotEq,
                    ">" => Operator::Gt,
                    ">=" => Operator::GtEq,
                    "<" => Operator::Lt,
                    "<=" => Operator::LtEq,
                    _ => {
                        return Err(QueryParserError::InvalidOperator(
                            caps["operator"].to_string(),
                        ))
                    }
                };

                Ok(Condition {
                    key: caps["key"].to_string(),
                    value: caps["value"].to_string(),
                    operator,
                })
            }
            None => Err(QueryParserError::InvalidCondition(query.to_string())),
        }
    }
}
