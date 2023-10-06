CREATE TABLE files
(
file_path text primary key not null,
id blob not null,
hash blob not null,
chunk_size int not null,
chunks int not null
)
