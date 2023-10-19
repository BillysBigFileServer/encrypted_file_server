# Billy's Encrypted File Server

## Usage
### Building the server
`nix build`

### Running the server
`nix run`

### Running the CLI
1. `nix develop` (to get all the dependencies necessary for the CLI)
2. `cd cli/`
3. `sqlx db create` (initializes the CLI's database)
4. `cargo run`
