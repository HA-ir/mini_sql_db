// Lexer - tokenizes SQL input

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Create,
    Table,
    Insert,
    Into,
    Select,
    From,
    Where,
    Values,
    Index,
    On,
    Delete,
    Update,
    Set,
    
    // Data types
    Int,
    Text,
    Float,
    
    // Operators
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    
    // Symbols
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Star,
    
    // Literals
    Identifier(String),
    IntLiteral(i64),
    FloatLiteral(f64),
    StringLiteral(String),
    
    // Special
    Eof,
}

pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace();

            if self.position >= self.input.len() {
                tokens.push(Token::Eof);
                break;
            }

            let token = self.next_token()?;
            
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            
            tokens.push(token);
        }

        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token, String> {
        if self.position >= self.input.len() {
            return Ok(Token::Eof);
        }

        let ch = self.current_char();

        // Single character tokens
        match ch {
            '(' => {
                self.advance();
                return Ok(Token::LeftParen);
            }
            ')' => {
                self.advance();
                return Ok(Token::RightParen);
            }
            ',' => {
                self.advance();
                return Ok(Token::Comma);
            }
            ';' => {
                self.advance();
                return Ok(Token::Semicolon);
            }
            '*' => {
                self.advance();
                return Ok(Token::Star);
            }
            '=' => {
                self.advance();
                return Ok(Token::Equals);
            }
            '>' => {
                self.advance();
                if self.position < self.input.len() && self.current_char() == '=' {
                    self.advance();
                    return Ok(Token::GreaterOrEqual);
                }
                return Ok(Token::GreaterThan);
            }
            '<' => {
                self.advance();
                if self.position < self.input.len() {
                    match self.current_char() {
                        '=' => {
                            self.advance();
                            return Ok(Token::LessOrEqual);
                        }
                        '>' => {
                            self.advance();
                            return Ok(Token::NotEquals);
                        }
                        _ => {}
                    }
                }
                return Ok(Token::LessThan);
            }
            '!' => {
                self.advance();
                if self.position < self.input.len() && self.current_char() == '=' {
                    self.advance();
                    return Ok(Token::NotEquals);
                }
                return Err("Unexpected character '!'".to_string());
            }
            _ => {}
        }

        // String literals
        if ch == '\'' || ch == '"' {
            return self.read_string(ch);
        }

        // Numbers
        if ch.is_ascii_digit() {
            return self.read_number();
        }

        // Identifiers and keywords
        if ch.is_alphabetic() || ch == '_' {
            return self.read_identifier();
        }

        Err(format!("Unexpected character: '{}'", ch))
    }

    fn read_string(&mut self, quote: char) -> Result<Token, String> {
        self.advance(); // Skip opening quote
        let mut value = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            
            if ch == quote {
                self.advance(); // Skip closing quote
                return Ok(Token::StringLiteral(value));
            }
            
            if ch == '\\' && self.position + 1 < self.input.len() {
                self.advance();
                let escaped = self.current_char();
                match escaped {
                    'n' => value.push('\n'),
                    't' => value.push('\t'),
                    '\\' => value.push('\\'),
                    '\'' => value.push('\''),
                    '"' => value.push('"'),
                    _ => value.push(escaped),
                }
                self.advance();
            } else {
                value.push(ch);
                self.advance();
            }
        }

        Err("Unterminated string literal".to_string())
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let mut value = String::new();
        let mut is_float = false;

        while self.position < self.input.len() {
            let ch = self.current_char();
            
            if ch.is_ascii_digit() {
                value.push(ch);
                self.advance();
            } else if ch == '.' && !is_float {
                is_float = true;
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if is_float {
            value.parse::<f64>()
                .map(Token::FloatLiteral)
                .map_err(|_| format!("Invalid float: {}", value))
        } else {
            value.parse::<i64>()
                .map(Token::IntLiteral)
                .map_err(|_| format!("Invalid integer: {}", value))
        }
    }

    fn read_identifier(&mut self) -> Result<Token, String> {
        let mut value = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            
            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Check if it's a keyword
        let token = match value.to_uppercase().as_str() {
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "VALUES" => Token::Values,
            "INDEX" => Token::Index,
            "ON" => Token::On,
            "DELETE" => Token::Delete,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "INT" => Token::Int,
            "TEXT" => Token::Text,
            "FLOAT" => Token::Float,
            _ => Token::Identifier(value),
        };

        Ok(token)
    }

    fn current_char(&self) -> char {
        self.input[self.position]
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() && self.current_char().is_whitespace() {
            self.advance();
        }
    }
}