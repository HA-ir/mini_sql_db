use mini_sql_db::repl::Repl;
use std::process;

fn main() {
    println!("Mini SQL Database v0.1.0");
    println!("Type '.help' for available commands, '.exit' to quit\n");

    let mut repl = Repl::new();
    
    if let Err(e) = repl.run() {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}