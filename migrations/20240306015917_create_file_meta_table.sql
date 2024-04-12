CREATE TABLE file_metadata (
id bigserial primary key unique,
encrypted_metadata bytea not null,
nonce bytea not null,
user_id bigint not null
);
