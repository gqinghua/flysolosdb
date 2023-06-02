use std::collections::HashMap;

use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, Values};

use crate::{error::error::{Result, SQLRiteError}, sql::db::table::{self, Table}};

/// 下面的结构表示已经解析过的INSERT查询
/// 并分解为“table_name”和“Vec<String>”表示“列”
/// ' Vec<Vec<String>> '表示要插入的' Rows '列表
#[derive(Debug)]
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}



impl InsertQuery {
pub fn new(statement: &Statement) -> Result<InsertQuery> {
    let mut tname: Option<String> = None;
        let mut columns: Vec<String> = vec![];
        let mut all_vals: Vec<Vec<String>> = vec![];

        if let Statement::Insert {
            table_name,
            columns: cols,
            source,
            ..
        } = statement{
            tname = Some(table_name.to_string());
            for c in cols {
                columns.push(c.to_string());
            }
            let Query { body, .. } = &**source;
            if let SetExpr::Values(values) = body.as_ref() {
                let Values { rows, .. } = values;
                for row in rows {
                    let mut value_set: Vec<String> = vec![];
                    for expr in row {
                        match expr {
                            Expr::Value(v) => match v {
                                Value::Number(n, _) => {
                                    value_set.push(n.to_string());
                                }
                                Value::Boolean(b) => match b {
                                    true => value_set.push("true".to_string()),
                                    false => value_set.push("false".to_string()),
                                },
                                Value::SingleQuotedString(sqs) => {
                                    value_set.push(sqs.to_string());
                                }
                                Value::Null => {
                                    value_set.push("Null".to_string());
                                }
                                _ => {}
                            },
                            Expr::Identifier(i) => {
                                value_set.push(i.to_string());
                            }
                            _ => {}
                        }
                    }
                    all_vals.push(value_set);
                }
                //持久化操作
            }
        }

        match tname {
                        Some(t) => Ok(InsertQuery {
                            table_name: t,
                            columns,
                            rows: all_vals,
                        }),
                        None => Err(SQLRiteError::Internal(
                            "Error parsing insert query".to_string(),
                        )),
                    }
                }
            }
            