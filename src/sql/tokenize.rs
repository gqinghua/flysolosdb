use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum Token {
    Select,
    From,
    Identifier(String),
}

pub fn tokenize(sql: &str) -> Vec<Token> {
    tracing::info!("tokens sql{:?}", sql);
    let mut tokens = Vec::new();
    let words = sql.split_whitespace();
    for word in words {
        match word {
            "SELECT" => tokens.push(Token::Select),
            "FROM" => tokens.push(Token::From),
            _ if word.chars().all(|c| c.is_alphanumeric() || c == '_') => {
                tokens.push(Token::Identifier(word.to_string()))
            }
            _ => {}
        }
    }
    tokens
}

pub fn parse(tokens: &[Token]) -> HashMap<String, Vec<String>> {
    let mut result = HashMap::new();
    let mut current_table = String::new();
    let mut current_columns = Vec::new();
    for token in tokens {
        match token {
            Token::Select => {}
            Token::From => {
                if!current_table.is_empty() {
                    result.insert(current_table.clone(), current_columns.clone());
                    current_table.clear();
                    current_columns.clear();
                }
            }
            Token::Identifier(ref name) => {
                if current_table.is_empty() {
                    current_table = name.clone();
                } else {
                    current_columns.push(name.clone());
                }
            }
        }
    }
    if!current_table.is_empty() {
        result.insert(current_table, current_columns);
    }
    result
}

