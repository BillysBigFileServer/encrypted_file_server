-- Add migration script here
CREATE table enc_keys (
username text primary key default null,
enc_key blob not null
)
