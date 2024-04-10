CREATE TABLE file_metadata (
id integer primary key autoincrement unique,
encrypted_metadata blob not null,
nonce blob not null,
user_id int not null
);
