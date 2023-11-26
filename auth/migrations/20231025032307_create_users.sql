create table users (
username text primary key not null,
password text not null,
salt text not null
)
