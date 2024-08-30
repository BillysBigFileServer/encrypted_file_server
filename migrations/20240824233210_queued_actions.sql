create table queued_actions (
    id serial primary key not null,
    action text not null,
    user_id bigint not null,
    execute_at timestamptz not null,
    status text not null
);

CREATE INDEX id_user_id_status ON queued_actions (id, user_id, status);
