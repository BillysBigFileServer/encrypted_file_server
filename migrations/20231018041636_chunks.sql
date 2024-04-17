CREATE TABLE chunks (
id text primary key unique not null,
hash text not null,
indice bigint not null,
chunk_size bigint not null,
nonce bytea not null,
user_id bigint not null
)
