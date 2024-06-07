FROM nixos/nix:latest as builder

COPY flake.nix flake.lock /tmp/build/
COPY rust-toolchain.toml /tmp/build/
COPY Cargo.toml Cargo.lock /tmp/build/
COPY .cargo/ /tmp/build/
WORKDIR /tmp/build

RUN nix --extra-experimental-features "nix-command flakes" --option filter-syscalls false build '.#build-deps-prod'

COPY src /tmp/build/src

RUN nix --extra-experimental-features "nix-command flakes" --option filter-syscalls false build '.?submodules=1 .#build-prod'

RUN mkdir -p /tmp/nix-store-closure
RUN cp -R $(nix-store -qR result/) /tmp/nix-store-closure

FROM rust:slim-bookworm as builder-sqlx
WORKDIR /build
RUN apt-get update && apt-get install -y pkg-config libssl-dev
RUN cargo install sqlx-cli --no-default-features --features postgres --root .

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install ca-certificates nix -y
RUN update-ca-certificates -f
COPY --from=builder /tmp/nix-store-closure /nix/store
COPY --from=builder /tmp/build/result /app

COPY --from=builder-sqlx /build/bin/sqlx /usr/bin/sqlx
COPY migrations /app/migrations

CMD ["/app/bin/file_server"]
