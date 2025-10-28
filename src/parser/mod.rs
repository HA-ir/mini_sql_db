// Parser module - converts SQL strings into AST

/// SQL data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Text,
    Float,
}

/// Column definition in a table
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// SQL Statement AST
#[derive(Debug)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<Column>,
    },
    CreateIndex {
        table_name: String,
        column_name: String,
    },
    Insert {
        table_name: String,
        values: Vec<Value>,
    },
    Select {
        table_name: String,
        columns: Vec<String>, // Empty vec means SELECT *
        where_clause: Option<WhereClause>,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereClause>,
    },
    Update {
        table_name: String,
        column: String,
        value: Value,
        where_clause: Option<WhereClause>,
    },
}

/// Represents a value in SQL
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Text(String),
    Float(f64),
    Null,
}

/// WHERE clause representation
#[derive(Debug)]
pub struct WhereClause {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

/// Comparison operators
#[derive(Debug)]
pub enum Operator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
}

pub mod lexer;
use lexer::{Lexer, Token};

/// Parse SQL string into Statement
pub fn parse(sql: &str) -> Result<Statement, String> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, position: 0 }
    }

    fn parse_statement(&mut self) -> Result<Statement, String> {
        let token = self.current_token();
        
        match token {
            Token::Create => {
                self.advance();
                let next = self.current_token();
                match next {
                    Token::Table => self.parse_create_table(),
                    Token::Index => self.parse_create_index(),
                    _ => Err(format!("Expected TABLE or INDEX after CREATE, got {:?}", next)),
                }
            }
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Delete => self.parse_delete(),
            Token::Update => self.parse_update(),
            _ => Err(format!("Unexpected token: {:?}", token)),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Table)?;
        
        let table_name = self.expect_identifier()?;
        
        self.expect_token(Token::LeftParen)?;
        
        let mut columns = Vec::new();
        
        loop {
            let col_name = self.expect_identifier()?;
            let col_type = self.parse_data_type()?;
            
            columns.push(Column {
                name: col_name,
                data_type: col_type,
            });
            
            if self.current_token() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        
        self.expect_token(Token::RightParen)?;
        
        Ok(Statement::CreateTable { table_name, columns })
    }

    fn parse_create_index(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Index)?;
        self.expect_token(Token::On)?;
        
        let table_name = self.expect_identifier()?;
        
        self.expect_token(Token::LeftParen)?;
        let column_name = self.expect_identifier()?;
        self.expect_token(Token::RightParen)?;
        
        Ok(Statement::CreateIndex { table_name, column_name })
    }

    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Insert)?;
        self.expect_token(Token::Into)?;
        
        let table_name = self.expect_identifier()?;
        
        self.expect_token(Token::Values)?;
        self.expect_token(Token::LeftParen)?;
        
        let mut values = Vec::new();
        
        loop {
            let value = self.parse_value()?;
            values.push(value);
            
            if self.current_token() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        
        self.expect_token(Token::RightParen)?;
        
        Ok(Statement::Insert { table_name, values })
    }

    fn parse_select(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Select)?;
        
        let columns = if self.current_token() == &Token::Star {
            self.advance();
            Vec::new() // Empty means SELECT *
        } else {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_identifier()?);
                
                if self.current_token() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            cols
        };
        
        self.expect_token(Token::From)?;
        let table_name = self.expect_identifier()?;
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        
        Ok(Statement::Select {
            table_name,
            columns,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Delete)?;
        self.expect_token(Token::From)?;
        
        let table_name = self.expect_identifier()?;
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        
        Ok(Statement::Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_update(&mut self) -> Result<Statement, String> {
        self.expect_token(Token::Update)?;
        
        let table_name = self.expect_identifier()?;
        
        self.expect_token(Token::Set)?;
        
        let column = self.expect_identifier()?;
        
        self.expect_token(Token::Equals)?;
        
        let value = self.parse_value()?;
        
        let where_clause = if self.current_token() == &Token::Where {
            self.advance();
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        
        Ok(Statement::Update {
            table_name,
            column,
            value,
            where_clause,
        })
    }

    fn parse_where_clause(&mut self) -> Result<WhereClause, String> {
        let column = self.expect_identifier()?;
        let operator = self.parse_operator()?;
        let value = self.parse_value()?;
        
        Ok(WhereClause {
            column,
            operator,
            value,
        })
    }

    fn parse_operator(&mut self) -> Result<Operator, String> {
        let token = self.current_token().clone();
        self.advance();
        
        match token {
            Token::Equals => Ok(Operator::Equals),
            Token::NotEquals => Ok(Operator::NotEquals),
            Token::GreaterThan => Ok(Operator::GreaterThan),
            Token::LessThan => Ok(Operator::LessThan),
            Token::GreaterOrEqual => Ok(Operator::GreaterOrEqual),
            Token::LessOrEqual => Ok(Operator::LessOrEqual),
            _ => Err(format!("Expected operator, got {:?}", token)),
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType, String> {
        let token = self.current_token().clone();
        self.advance();
        
        match token {
            Token::Int => Ok(DataType::Int),
            Token::Text => Ok(DataType::Text),
            Token::Float => Ok(DataType::Float),
            _ => Err(format!("Expected data type, got {:?}", token)),
        }
    }

    fn parse_value(&mut self) -> Result<Value, String> {
        let token = self.current_token().clone();
        self.advance();
        
        match token {
            Token::IntLiteral(n) => Ok(Value::Int(n)),
            Token::FloatLiteral(f) => Ok(Value::Float(f)),
            Token::StringLiteral(s) => Ok(Value::Text(s)),
            _ => Err(format!("Expected value, got {:?}", token)),
        }
    }

    fn expect_token(&mut self, expected: Token) -> Result<(), String> {
        if self.current_token() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(format!(
                "Expected {:?}, got {:?}",
                expected,
                self.current_token()
            ))
        }
    }

    fn expect_identifier(&mut self) -> Result<String, String> {
        match self.current_token().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            token => Err(format!("Expected identifier, got {:?}", token)),
        }
    }

    fn current_token(&self) -> &Token {
        if self.position < self.tokens.len() {
            &self.tokens[self.position]
        } else {
            &Token::Eof
        }
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }
}