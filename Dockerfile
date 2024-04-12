FROM nixos/nix:latest as builder

COPY src /tmp/build/src
COPY flake.nix flake.lock /tmp/build/
COPY rust-toolchain.toml /tmp/build/
COPY Cargo.toml Cargo.lock /tmp/build/
COPY migrations /tmp/build/migrations

WORKDIR /tmp/build

RUN nix --extra-experimental-features "nix-command flakes" --option filter-syscalls false build '.?submodules=1'

RUN mkdir -p /tmp/nix-store-closure
RUN cp -R $(nix-store -qR result/) /tmp/nix-store-closure

FROM scratch
WORKDIR /app
COPY --from=builder /tmp/nix-store-closure /nix/store
COPY --from=builder /tmp/build/result /app
COPY migrations /app/migrations

CMD ["/app/bin/file_server"]
