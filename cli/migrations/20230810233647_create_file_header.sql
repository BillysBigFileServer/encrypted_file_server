CREATE TABLE files
(
id blob primary key not null,
file_path text default null,
hash blob not null,
chunk_size int not null,
chunks int not null
)
