create table keys (
username text primary key not null,
enc_key blob not null,
macaroon text not null
)
