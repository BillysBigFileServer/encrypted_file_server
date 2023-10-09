CREATE TABLE chunks (
hash text primary key not null,
id text not null,
indice int not null,
uploaded boolean default false,
chunk_size int not null,
file_hash blob not null
)
