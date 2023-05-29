extern crate clap;
#[macro_use]
extern crate prettytable;
extern crate sqlparser;

mod error;
mod meta_command;
mod repl;
mod sql;

use meta_command::handle_meta_command;
use repl::{get_command_type, get_config, CommandType};

use sql::db::database::Database;
use sql::process_command;
use clap::{crate_authors, crate_description, crate_name, crate_version, Command};
use rustyline::error::ReadlineError;
use rustyline::{Cmd, Editor};
use rustyline::{DefaultEditor, Result};

// 测试
use crate::repl::MyHelper;
use rustyline::completion::FilenameCompleter;
use rustyline::highlight::MatchingBracketHighlighter;
use rustyline::hint::HistoryHinter;
use rustyline::validate::MatchingBracketValidator;
use rustyline::KeyEvent;
fn main() -> rustyline::Result<()> {
    // 设置日志

    env_logger::init();

    // 使用默认配置启动Rustyline
    let config = get_config();
    // // 获得一个新的Rustyline助手
    // // 使用设置配置和设置帮助器初始化Rustyline编辑器
    let myh = MyHelper {
        completer: FilenameCompleter::new(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter {},
        colored_prompt: "".to_owned(),
        validator: MatchingBracketValidator::new(),
    };

    let mut repl = Editor::with_config(config)?;

    repl.set_helper(Some(myh));

    repl.bind_sequence(KeyEvent::alt('n'), Cmd::HistorySearchForward);
    repl.bind_sequence(KeyEvent::alt('p'), Cmd::HistorySearchBackward);

    // 此方法将历史文件加载到内存中
    // 如果它不存在，则创建一个
    // 待办事项:检查历史文件大小，如果太大，清理它。
    if repl.load_history("history").is_err() {
        println!("No previous history.");
    }

    // 友好的介绍信息的用户
    println!(
        "{} - {}\n{}{}{}{}",
        crate_name!(),
        crate_version!(),
        "Enter .exit to quit.\n",
        "Enter .help for usage hints.\n",
        "Connected to a transient in-memory database.\n",
        "Use '.open FILENAME' to reopen on a persistent database."
    );

    let mut db = Database::new(&"tempdb".to_string());

    loop {
        let p = format!("sqlrite> ");
        repl.helper_mut().expect("No helper").colored_prompt = format!("\x1b[1;32m{p}\x1b[0m");
            
        let readline = repl.readline(&p);
        match readline {
            Ok(command) => {
                repl.add_history_entry(command.as_str());
                //解析用户输入和repl::CommandType的返回和枚举
                match get_command_type(&command.trim().to_owned()) {
                    CommandType::SQLCommand(_cmd) => {
                        // process_command负责标记、解析和执行
                        //返回一个Result<String的SQL语句，SQLRiteError>
                        let _ = match process_command(&command, &mut db) {
                            Ok(response) => println!("{}", response),
                            Err(err) => eprintln!("An error occured: {}", err),
                        };
                    }
                    CommandType::MetaCommand(cmd) => {
                        // handle_meta_command解析并执行metcommand
                        let _ = match handle_meta_command(cmd, &mut repl) {
                            Ok(response) => println!("{}", response),
                            Err(err) => eprintln!("An error occured: {}", err),
                        };
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("Encountered Eof");
                break;
            }
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }
    repl.append_history("history");

    Ok(())
}
