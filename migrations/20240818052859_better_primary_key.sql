ALTER TABLE chunks RENAME CONSTRAINT chunks_pkey TO chunks_pkeyold;
CREATE UNIQUE INDEX chunks_pkey ON chunks (id, user_id);
ALTER TABLE chunks DROP CONSTRAINT chunks_pkeyold;
ALTER TABLE chunks ADD PRIMARY KEY USING INDEX chunks_pkey;
ALTER TABLE chunks ADD CONSTRAINT unique_id_user_id UNIQUE (id, user_id);
