alter table chunks add column encrypted_metadata bytea;
alter table chunks add column enc_chunk_size bigint;
alter table chunks alter column hash drop not null;
alter table chunks alter column indice drop not null;
alter table chunks alter column chunk_size drop not null;
alter table chunks alter column nonce drop not null;
