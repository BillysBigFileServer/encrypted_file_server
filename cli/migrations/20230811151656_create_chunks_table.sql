CREATE TABLE chunks (
hash blob primary key not null,
file_id blob not null,
id int not null,
uploaded boolean default false,
chunk_size int not null
)
