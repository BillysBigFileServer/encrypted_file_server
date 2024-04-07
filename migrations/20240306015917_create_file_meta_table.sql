CREATE TABLE file_metadata (
id serial primary key unique not null,
encrypted_metadata blob not null,
nonce blob not null,
user_id int not null
);
