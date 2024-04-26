CREATE TABLE file_metadata (
id text not null,
encrypted_metadata bytea not null,
user_id bigint not null,
primary key (id, user_id)
);
