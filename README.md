# Multi-DB SQL Query System

A natural language interface for querying industrial IoT databases with role-based access control.

## Overview

This system enables natural language queries across three SQLite databases (sensors, maintenance, revenue) using LangChain and Ollama. Access to tables is controlled by user roles.

## Prerequisites

- Python 3.13
- Ollama with llama3 model (`ollama pull llama3:latest`)

## Installation

```bash
pipenv install
```

## Setup

Generate the databases:

```bash
cd src
python generate_data.py
```

## Usage

```bash
cd src
python main.py
```

Access the Gradio interface to query the databases using natural language. Select a role to simulate different access permissions.

## Roles

- **SensorViewer**: Access to sensor data only
- **MaintenanceManager**: Access to sensors and maintenance data
- **RevenueAnalyst**: Access to sensors and revenue data
- **PlantDirector**: Full access to all databases

## Project Structure

```
rag_sql_project/
├── src/
│   ├── main.py              # Gradio interface
│   ├── database_manager.py  # Database connection and RBAC
│   ├── llm_engine.py        # LangChain SQL chain
│   └── generate_data.py     # Database generation script
├── data/                    # SQLite databases
├── Pipfile
└── README.md
```

## License

MIT
