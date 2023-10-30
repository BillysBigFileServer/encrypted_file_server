# Billy's Encrypted File Server

## Architecture
There are currently three components to the `befs`; `auth`, `file_server`, and `cli`.
 `auth` handles authentication, including storing authentication information, as well as giving out [macaroons](http://tech.tmh.io/concept/2016/06/07/macaroons-a-new-flavor-for-authorization.html). If you're a pentester, this is a juicy target ;). `file_server` handles uploading and downloading files for users, storing them, and (TODO) limiting the amount of storage each individual user can store. Because of the fact that files are supposed to be, well, e2e encrypted, the `cli` does a lot of heavy lifting. It deals with compressing and encrypting files (in that order!), as well as keeping information regarding how chunks should be strung together in order to be rebuilt. Oh about chunks, `bfsp` is the protocol library that glues all of this together. It deals with how exactly files should be split apart into chunks and put back together, as well as how exactly to speak with the authentication server. At the moment, all communication with the `file_server` is done over raw TCP (using `bfsp` for primitives), and all communication with `auth` is done over HTTP.
 
# Dependencies
I make heavy use of the [nix package manager](https://nixos.org/download). So if you want to work on the project, I recommend installing that. Alternatively, if you just want to run the server, you can use Docker.

## Usage
Remember that there are two parts two the server, `auth` and `file_server`. While you can run both components individually, you probably want to be running both ;).

### Building the server
Enter into either the `auth/` directory, or the root directory, and just run `nix build`

You can also use Docker by running `docker buildx build . -t billys_file_server`

### Running the server
Before running the server, you need to create an empty `data.db` file in the root directory, as well as in `auth/`. 

Enter into either the `auth/` directory, or the root directory, and just run `nix run`

As for Docker users, if you already built the Docker image and tagged it with `billys_file_server`, you can run it using `docker run -it billys_file_server`

### Running the CLI
1. `nix develop` (to get all the dependencies necessary for the CLI)
2. `cd cli/`
3. `sqlx db create` (initializes the CLI's database)
4. `cargo run`
