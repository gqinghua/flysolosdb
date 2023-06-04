use crate::error::error::{Result, SQLRiteError};

use crate::repl::MyHelper;
use rustyline::history::DefaultHistory;
use rustyline::{Cmd, CompletionType, Config, EditMode, Editor, KeyEvent};
use std::fmt;
/**
 * 元数据
 */
#[derive(Debug, PartialEq)]
pub enum MetaCommand {
    Exit,
    Help,
    Open(String),
    Unknown,
}

/// 负责将字体转换为格式化文本的特性。
impl fmt::Display for MetaCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MetaCommand::Exit => f.write_str(".exit"),
            MetaCommand::Help => f.write_str(".help"),
            MetaCommand::Open(_) => f.write_str(".open"),
            MetaCommand::Unknown => f.write_str("Unknown command"),
        }
    }
}

impl MetaCommand {
    pub fn new(command: String) -> MetaCommand {
        let args: Vec<&str> = command.split_whitespace().collect();
        let cmd = args[0].to_owned();
        match cmd.as_ref() {
            ".exit" => MetaCommand::Exit,
            ".help" => MetaCommand::Help,
            ".open" => MetaCommand::Open(command),
            _ => MetaCommand::Unknown,
        }
    }
}

pub fn handle_meta_command(
    command: MetaCommand,
    repl: &mut Editor<MyHelper, DefaultHistory>,
) -> Result<String> {
    match command {
        MetaCommand::Exit => {
            repl.append_history("history").unwrap();
            std::process::exit(0)
        }
        MetaCommand::Help => Ok(format!(
            "{}{}{}{}{}{}{}{}",
            "Special commands:\n",
            ".help            - Display this message\n",
            ".open <FILENAME> - Close existing database and reopen FILENAME\n",
            ".save <FILENAME> - Write in-memory database into FILENAME\n",
            ".read <FILENAME> - Read input from FILENAME\n",
            ".tables          - List names of tables\n",
            ".ast <QUERY>     - Show the abstract syntax tree for QUERY.\n",
            ".exit            - Quits this application"
        )),
        MetaCommand::Open(args) => Ok(format!("To be implemented: {}", args)),
        MetaCommand::Unknown => Err(SQLRiteError::UnknownCommand(format!(
            "Unknown command or invalid arguments. Enter '.help'"
        ))),
    }
}
