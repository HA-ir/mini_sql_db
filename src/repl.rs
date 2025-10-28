use std::io::{self, Write};
use crate::parser;
use crate::storage::Database;

/// REPL (Read-Eval-Print Loop) for the database
pub struct Repl {
    running: bool,
    database: Database,
}

impl Repl {
    /// Create a new REPL instance
    pub fn new() -> Self {
        // Try to load existing database from disk
        let database = match Database::load_from_disk() {
            Ok(db) => {
                let table_count = db.list_tables().len();
                if table_count > 0 {
                    println!("Loaded {} existing table(s) from disk", table_count);
                }
                db
            }
            Err(e) => {
                eprintln!("Could not load database from disk: {}", e);
                println!("Starting with empty database");
                Database::new()
            }
        };

        Self { 
            running: true,
            database,
        }
    }

    /// Main REPL loop
    pub fn run(&mut self) -> io::Result<()> {
        while self.running {
            // Print prompt
            print!("mydb> ");
            io::stdout().flush()?;

            // Read user input
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;

            let input = input.trim();

            // Skip empty lines
            if input.is_empty() {
                continue;
            }

            // Handle meta commands (starting with .)
            if input.starts_with('.') {
                self.handle_meta_command(input);
                continue;
            }

            // Handle SQL commands
            self.handle_sql_command(input);
        }

        Ok(())
    }

    /// Handle meta commands like .exit, .help
    fn handle_meta_command(&mut self, command: &str) {
        match command {
            ".exit" | ".quit" => {
                println!("Goodbye!");
                self.running = false;
            }
            ".help" => {
                self.print_help();
            }
            ".tables" => {
                let tables = self.database.list_tables();
                if tables.is_empty() {
                    println!("No tables in database");
                } else {
                    println!("Tables:");
                    for table in tables {
                        println!("  - {}", table);
                    }
                }
            }
            _ => {
                println!("Unknown command: {}. Type .help for available commands.", command);
            }
        }
    }

    /// Handle SQL commands
    fn handle_sql_command(&mut self, sql: &str) {
        match parser::parse(sql) {
            Ok(statement) => {
                // Convert statement to plan
                match crate::planner::plan(statement) {
                    Ok(plan) => {
                        // Execute plan
                        match crate::executor::execute(plan, &mut self.database) {
                            Ok(result) => {
                                let output = crate::executor::format_results(result);
                                println!("{}", output);
                            }
                            Err(e) => {
                                println!("✗ Execution error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("✗ Planning error: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("✗ Parse error: {}", e);
            }
        }
    }

    /// Print help information
    fn print_help(&self) {
        println!("Available commands:");
        println!("  .help          - Show this help message");
        println!("  .exit/.quit    - Exit the database");
        println!("  .tables        - List all tables");
        println!("\nSupported SQL:");
        println!("  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...)");
        println!("  INSERT INTO table_name VALUES (val1, val2, ...)");
        println!("  SELECT * FROM table_name");
        println!("  SELECT col1, col2 FROM table_name WHERE col = value");
    }
}

impl Default for Repl {
    fn default() -> Self {
        Self::new()
    }
}