# Sakila DB Assistant 🦙

A natural language interface for querying the Sakila DVD rental database using LangChain and Ollama.

## Features

- Natural language to SQL query conversion
- Interactive web interface using Gradio
- Dynamic schema retrieval from SQLite database
- Local LLM processing with Ollama (Llama2)
- Educational tool for learning SQL and database concepts

## Prerequisites

- Python 3.8+
- [Ollama](https://ollama.com/) installed and running
- Llama2 model downloaded (`ollama pull llama2`)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd rag_sql_project
```

2. Install dependencies:
```bash
pipenv install
# or
pip install -r requirements.txt
```

3. Ensure Ollama is running with Llama2:
```bash
ollama serve
ollama pull llama2
```

## Usage

Run the application:
```bash
cd src
python main.py
```

The Gradio interface will launch in your browser where you can ask natural language questions about the Sakila database, such as:

- "How many films are in the database?"
- "Which actors have appeared in the most films?"
- "What are the top 5 highest-grossing film categories?"

## Database

The application uses the Sakila sample database, a fictional DVD rental store database that includes:

- **Tables**: actor, film, customer, rental, inventory, payment, and more
- **Relationships**: Many-to-many relationships between films and actors, categories, etc.
- **Views**: Pre-defined queries for common data access patterns

## Architecture

1. **Schema Retrieval**: Dynamically fetches database schema on startup
2. **Query Generation**: Uses LangChain + Llama2 to convert natural language to SQL
3. **Query Execution**: Runs generated SQL queries safely against the SQLite database
4. **Response Generation**: Formats results into natural language responses

## Project Structure

```
rag_sql_project/
├── src/
│   └── main.py              # Main application
├── data/
│   └── database/
│       └── sakila_master.db # SQLite database
├── Pipfile                  # Python dependencies
└── README.md
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is for educational purposes. The Sakila database is a sample database provided by MySQL for learning and testing.
