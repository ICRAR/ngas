--
-- Adds the new delivery queue table needed in 10.1
--
create table ngas_subscr_delivery_queue
(
    subscr_id       varchar(255)  not null,
    file_id         varchar(64)   not null,
    file_version    int           not null default 1,
    disk_id         varchar(128)  not null,
    constraint delivery_queue_idx primary key(subscr_id, file_id, file_version, disk_id)
);