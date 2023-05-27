use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, Values};

use crate::error::error::{Result, SQLRiteError};

/// 下面的结构表示已经解析过的INSERT查询
/// 并分解为“table_name”和“Vec<String>”表示“列”
/// ' Vec<Vec<String>> '表示要插入的' Rows '列表
#[derive(Debug)]
pub struct InsertQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

//利用下方的sql-sqlparser
// impl InsertQuery {
//     pub fn new(statement: &Statement) -> Result<InsertQuery> {
//         #[allow(unused_assignments)]
//         let mut tname: Option<String> = None;
//         let mut columns: Vec<String> = vec![];
//         let mut all_values: Vec<Vec<String>> = vec![];

//         match statement {
//             Statement::Insert {
//                 table_name,
//                 columns: cols,
//                 source,
//                 ..
//             } => {
//                 tname = Some(table_name.to_string());
//                 for col in cols {
//                     columns.push(col.to_string());
//                 }

//                 match &**source {
//                     Query {
//                         body,
//                         order_by: _order_by,
//                         limit: _limit,
//                         offset: _offset,
//                         fetch: _fetch,
//                         ..
//                     } => {
//                         if let SetExpr::Values(values) = body {
//                             #[allow(irrefutable_let_patterns)]
//                             if let Values(expressions) = values {
//                                 for i in expressions {
//                                     let mut value_set: Vec<String> = vec![];
//                                     for e in i {
//                                         match e {
//                                             Expr::Value(v) => match v {
//                                                 Value::Number(n, _) => {
//                                                     value_set.push(n.to_string());
//                                                 }
//                                                 Value::Boolean(b) => match *b {
//                                                     true => value_set.push("true".to_string()),
//                                                     false => value_set.push("false".to_string()),
//                                                 },
//                                                 Value::SingleQuotedString(sqs) => {
//                                                     value_set.push(sqs.to_string());
//                                                 }
//                                                 Value::Null => {
//                                                     value_set.push("Null".to_string());
//                                                 }
//                                                 _ => {}
//                                             },
//                                             Expr::Identifier(i) => {
//                                                 value_set.push(i.to_string());
//                                             }
//                                             _ => {}
//                                         }
//                                     }
//                                     all_values.push(value_set);
//                                 }
//                             }
//                         }




//                     }
//                 }
//             }
//             _ => {
//                 return Err(SQLRiteError::Internal(
//                     "Error parsing insert query".to_string(),
//                 ))
//             }
//         }

//         match tname {
//             Some(t) => Ok(InsertQuery {
//                 table_name: t,
//                 columns,
//                 rows: all_values,
//             }),
//             None => Err(SQLRiteError::Internal(
//                 "Error parsing insert query".to_string(),
//             )),
//         }
//     }
// }

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
        } = statement
        {
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
            