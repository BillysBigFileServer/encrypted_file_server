# Billy's Encrypted File Server

## Usage
### Building the server
`nix build`

### Running the server
`nix run`

### Running the CLI
`nix develop` (to get all the dependencies necessary for the CLI)
`cd cli/`
`sqlx db create` (initializes the CLI's database or resets it if it already e)
`cargo run`
