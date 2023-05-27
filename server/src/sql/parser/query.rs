use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Offset, Query,
    SelectItem::{ExprWithAlias, QualifiedWildcard, UnnamedExpr, Wildcard},
    SetExpr, Statement, TableFactor, TableWithJoins, Value, Values,
};

use crate::error::error::{Result, SQLRiteError};

#[derive(Debug, Clone, PartialEq)]
pub enum Binary {
    NotEq,
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug)]
pub enum Operator {
    Binary(Binary),
}

#[derive(Debug)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

impl From<BinaryOperator> for Operator {
    fn from(b: BinaryOperator) -> Self {
        match b {
            BinaryOperator::Eq => Operator::Binary(Binary::Eq),
            BinaryOperator::NotEq => Operator::Binary(Binary::NotEq),
            BinaryOperator::Gt => Operator::Binary(Binary::Gt),
            BinaryOperator::GtEq => Operator::Binary(Binary::GtEq),
            BinaryOperator::Lt => Operator::Binary(Binary::Lt),
            BinaryOperator::LtEq => Operator::Binary(Binary::LtEq),
            _ => panic!("Converting from unsupported BinaryOperator"),
        }
    }
}

#[derive(Debug)]
pub struct Expression {
    pub left: String,
    pub right: String,
    pub op: Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SelectItem {
    QualifiedWildcard(String),
    Wildcard,
}

#[derive(Debug)]
pub struct SelectQuery {
    pub from: String,
    pub projection: Vec<String>,
    pub where_expressions: Vec<Expression>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

impl SelectQuery {
    fn add_ops(
        op: &BinaryOperator,
        where_expressions: &mut Vec<Expression>,
        col_name: &Ident,
        n: &String,
    ) {
        match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::GtEq => {
                where_expressions.push(Expression {
                    left: col_name.to_string(),
                    right: n.to_string(),
                    op: Operator::from(op.clone()),
                });
            }
            _ => {
                panic!("cannot parse select query");
            }
        };
    }

    pub fn new(statement: &Statement) -> Result<SelectQuery> {
        let mut table_name: Option<String> = None;
        let mut projection: Vec<String> = vec![];
        let mut where_expressions: Vec<Expression> = vec![];
        let mut offset: Option<u64> = None;
        let mut limit: Option<u64> = None;

        match statement {
            Statement::Query(bq) => {
                if let Some(bq_offset) = &bq.offset {
                    let value = &bq_offset.value;
                    dbg!(value);
                    if let Expr::Value(Value::Number(n, _)) = value {
                        offset = Some(n.parse::<u64>().unwrap());
                    }
                }
                if let Some(Expr::Value(Value::Number(n, _))) = &bq.limit {
                    limit = Some(n.parse::<u64>().unwrap());
                }
                match &*(bq).body {
                    SetExpr::Select(select) => {
                        for p in &(select).projection {
                            match p {
                                UnnamedExpr(exp) => match exp {
                                    Expr::Identifier(i) => {
                                        projection.push(i.to_string());
                                    }
                                    _ => {
                                        println!(
                                            "Failing to parse expression in the where clause.\
                                         It's probably not an identifier or a value.\
                                         Cannot parse nested expressions :( ."
                                        );
                                    }
                                },
                                QualifiedWildcard(obj_name, _) => {
                                    println!("Found qualified wildcard in the expression. Wildcard name is  {}", obj_name);
                                }
                                Wildcard(_) => {
                                    projection.push("*".to_string());
                                }
                                ExprWithAlias { expr, .. } => match expr {
                                    Expr::Identifier(i) => {
                                        projection.push(i.to_string());
                                    }
                                    _ => {
                                        println!("Detected expression with alias. Cannot parse expression with alias.");
                                    }
                                },
                            }
                        }

                        for f in &select.from {
                            match &f.relation {
                                TableFactor::Table { name, alias, .. } => {
                                    table_name = Some(name.to_string());
                                    match alias {
                                        Some(alias) => println!("alias = {}", alias),
                                        None => println!("No table alias"),
                                    }
                                }
                                _ => println!("Nested join or derived tables"),
                            }
                        }

                        match &select.selection {
                            Some(where_expression) => {
                                println!("where expression in select.rs = {:?}", where_expression);
                                if let Expr::BinaryOp { left, op, right } = where_expression {
                                    if let Expr::Identifier(col_name) = &(**left) {
                                        if let Expr::Value(v) = &(**right) {
                                            if let Value::Number(n, _) = v {
                                                Self::add_ops(
                                                    op,
                                                    &mut where_expressions,
                                                    col_name,
                                                    n,
                                                );
                                            }
                                            if let Value::NationalStringLiteral(n) = v {
                                                Self::add_ops(
                                                    op,
                                                    &mut where_expressions,
                                                    col_name,
                                                    n,
                                                );
                                            }
                                            if let Value::SingleQuotedString(n) = v {
                                                Self::add_ops(
                                                    op,
                                                    &mut where_expressions,
                                                    col_name,
                                                    n,
                                                );
                                            }
                                        }

                                        if let Expr::Identifier(v) = &(**right) {
                                            let n = &v.to_string();
                                            Self::add_ops(op, &mut where_expressions, col_name, n);
                                        }
                                    };
                                }
                            }
                            None => {}
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }

        match table_name {
            Some(name) => Ok(SelectQuery {
                from: name,
                projection,
                where_expressions,
                offset,
                limit,
            }),
            None => todo!(),
        }
    }

    pub fn insert_projections(&mut self, projection: Vec<String>) -> &mut SelectQuery {
        self.projection = projection;
        self
    }
}
