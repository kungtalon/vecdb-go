# vecdb-go

## Overview

`vecdb-go` is a Go implementation of a vector database, designed to efficiently store, search, and manage high-dimensional vectors. This project aims to provide a robust and scalable solution for applications requiring vector similarity search and manipulation.

## Project Structure

The project is organized into several directories, each serving a specific purpose:

- **cmd/server**: Contains the entry point for the application, initializing the server and handling requests.
- **internal/api**: Implements the REST API, including handlers, routes, and data types.
- **internal/config**: Manages application configuration, loading settings from `config.toml`.
- **internal/filter**: Implements filtering logic for the vector database.
- **internal/index**: Contains various indexing methods, including flat and HNSW indexing.
- **internal/persistence**: Handles data persistence, saving and loading vector data.
- **internal/scalar**: Provides scalar operations and utilities.
- **internal/vecdb**: Implements the core database logic and vector operations.

## Getting Started

### Prerequisites

- Go 1.16 or later
- Dependencies specified in `go.mod`

### Installation

1. Clone the repository:

   ```
   git clone https://github.com/yourusername/vecdb-go.git
   cd vecdb-go
   ```

2. Install dependencies:

   ```
   go mod tidy
   ```

3. Create a configuration file named `config.toml` in the root directory. Here is an example configuration:

   ```toml
   [database]
   path = "path/to/database"

   [server]
   port = 8080
   search_url_suffix = "/search"
   upsert_url_suffix = "/upsert"
   log_level = "info"
   ```

### Running the Application

To start the server, run the following command:

```
go run cmd/server/main.go
```

The server will listen on the specified port (default: 8080).

### API Endpoints

- **POST /search**: Searches for vectors based on the provided query.
- **POST /upsert**: Inserts or updates vectors in the database.

### Testing

Unit tests are provided for each component of the application. To run the tests, use:

```
go test ./...
```

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.