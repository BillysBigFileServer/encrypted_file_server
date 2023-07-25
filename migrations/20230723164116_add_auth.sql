-- Add migration script here
create table user_auth (
    id integer primary key,
    username text not null,
    password text not null,
    salt text not null
)
