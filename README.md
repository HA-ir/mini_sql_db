# Mini SQL Database

A lightweight, educational SQL database implementation written in Rust. This project demonstrates core database concepts including SQL parsing, query planning, execution, indexing, and persistent storage.

## Features

- **SQL Support**: Implements a subset of SQL including CREATE, INSERT, SELECT, UPDATE, and DELETE
- **Data Types**: Supports INT, TEXT, and FLOAT data types
- **B-Tree Indexing**: Fast lookups using B-tree indexes on columns
- **Persistent Storage**: Data is saved to disk and automatically loaded on startup
- **Interactive REPL**: Command-line interface for executing SQL queries
- **Query Planning**: Converts SQL statements into optimized execution plans
- **WHERE Clause Support**: Filter data with comparison operators (=, !=, <, >, <=, >=)

## Installation

### Prerequisites

- Rust 1.70 or higher
- Cargo (comes with Rust)

### Building from Source

```bash
# Clone the repository
git clone git@github.com:HA-ir/mini_sql_db.git
cd mini_sql_db

# Build the project
cargo build --release

# Run the database
cargo run --release
```

## Usage

### Starting the Database

```bash
cargo run
```

This will start the interactive REPL:

```
Mini SQL Database v0.1.0
Type '.help' for available commands, '.exit' to quit

mydb>
```

### Meta Commands

- `.help` - Show available commands
- `.exit` or `.quit` - Exit the database
- `.tables` - List all tables in the database

### SQL Commands

#### CREATE TABLE

Create a new table with columns and their data types:

```sql
CREATE TABLE users (id INT, name TEXT, age INT);
CREATE TABLE products (id INT, name TEXT, price FLOAT);
```

#### CREATE INDEX

Create a B-tree index on a column for faster queries:

```sql
CREATE INDEX ON users (id);
CREATE INDEX ON products (price);
```

#### INSERT

Insert data into a table:

```sql
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO products VALUES (1, 'Laptop', 999.99);
```

#### SELECT

Query data from tables:

```sql
-- Select all columns
SELECT * FROM users;

-- Select specific columns
SELECT name, age FROM users;

-- Select with WHERE clause
SELECT * FROM users WHERE age > 25;
SELECT name FROM users WHERE id = 1;
SELECT * FROM products WHERE price <= 1000.0;
```

Supported comparison operators:
- `=` (equals)
- `!=` or `<>` (not equals)
- `<` (less than)
- `>` (greater than)
- `<=` (less than or equal)
- `>=` (greater than or equal)

#### UPDATE

Modify existing rows:

```sql
-- Update all rows
UPDATE users SET age = 31;

-- Update with condition
UPDATE users SET age = 26 WHERE id = 2;
UPDATE products SET price = 899.99 WHERE name = 'Laptop';
```

#### DELETE

Remove rows from a table:

```sql
-- Delete specific rows
DELETE FROM users WHERE age < 25;

-- Delete all rows
DELETE FROM users;
```

## Architecture

The project is organized into several modules:

### Parser (`src/parser/`)

- **Lexer** (`lexer.rs`): Tokenizes SQL input into a stream of tokens
- **Parser** (`mod.rs`): Converts tokens into an Abstract Syntax Tree (AST)

### Planner (`src/planner/`)

Transforms the AST into an optimized execution plan.

### Executor (`src/executor/`)

Executes query plans against the database and formats results.

### Storage (`src/storage/`)

- **Table Management** (`mod.rs`): In-memory table storage and operations
- **B-Tree Indexes** (`btree.rs`): Index implementation for fast lookups
- **Disk Persistence** (`disk.rs`): Serialization and deserialization of tables

### REPL (`src/repl.rs`)

Interactive command-line interface for the database.

## Data Storage

Tables are automatically saved to the `data/` directory in `.tbl` files. The format includes:

- **Schema line**: Column definitions (e.g., `id:INT,name:TEXT,age:INT`)
- **Data lines**: Row values separated by `|` (e.g., `1|Alice|30`)

Data is automatically loaded when the database starts.

## Example Session

```sql
mydb> CREATE TABLE employees (id INT, name TEXT, salary FLOAT);
Table 'employees' created successfully

mydb> INSERT INTO employees VALUES (1, 'Alice', 75000.0);
1 row inserted

mydb> INSERT INTO employees VALUES (2, 'Bob', 65000.0);
1 row inserted

mydb> INSERT INTO employees VALUES (3, 'Charlie', 80000.0);
1 row inserted

mydb> SELECT * FROM employees;
+----+---------+----------+
| id | name    | salary   |
+----+---------+----------+
| 1  | Alice   | 75000.00 |
| 2  | Bob     | 65000.00 |
| 3  | Charlie | 80000.00 |
+----+---------+----------+
3 row(s) returned

mydb> SELECT name FROM employees WHERE salary > 70000.0;
+---------+
| name    |
+---------+
| Alice   |
| Charlie |
+---------+
2 row(s) returned

mydb> CREATE INDEX ON employees (id);
Index created on column 'id' of table 'employees'

mydb> UPDATE employees SET salary = 85000.0 WHERE id = 3;
1 row(s) updated

mydb> .tables
Tables:
  - employees

mydb> .exit
Goodbye!
```

## Testing

Run the test suite:

```bash
cargo test
```

## Limitations

This is an educational project and has several limitations:

- No support for JOINs, GROUP BY, or aggregate functions
- Single-threaded execution only
- No transaction support or ACID guarantees
- Limited SQL syntax support
- No user authentication or permissions
- WHERE clauses only support simple comparisons (no AND/OR)
- No support for NULL constraints or default values

## Future Enhancements

Potential improvements for the project:

- [ ] Add support for compound WHERE clauses (AND, OR)
- [ ] Implement JOIN operations
- [ ] Add aggregate functions (COUNT, SUM, AVG, etc.)
- [ ] Support for ORDER BY and LIMIT
- [ ] Transaction support with BEGIN/COMMIT/ROLLBACK
- [ ] Multi-threading and concurrent access
- [ ] Query optimization and statistics
- [ ] More data types (BOOLEAN, DATE, TIMESTAMP)
- [ ] PRIMARY KEY and FOREIGN KEY constraints
- [ ] ALTER TABLE support
- [ ] Prepared statements

## Dependencies

- `nom` (7.1): Parser combinator library for building the SQL lexer and parser

## License

This project is provided as-is for educational purposes.

## Contributing

This is an educational project. Feel free to fork and experiment with your own database features!

## Acknowledgments

Built to demonstrate fundamental database concepts including:
- Query parsing and lexical analysis
- Query planning and optimization
- Storage engine design
- Index structures (B-trees)
- File-based persistence