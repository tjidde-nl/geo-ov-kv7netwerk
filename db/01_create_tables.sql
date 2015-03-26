set session authorization geo_ov;

create schema data;

alter database kv7netwerk set search_path=data,public;

set search_path=data,public;

create table netwerk(
    id serial,
    state varchar not null,
    processed_date timestamp,
    schema varchar,
    log text,
    primary key (id)
);

create table data(
    id serial,
    recv_date timestamp not null,
    filename varchar not null,
    file_md5 varchar,
    state varchar not null,
    state_date timestamp not null,
    data_owner varchar,
    data_date timestamp,
    log text,
    extent box2d,
    netwerk integer references netwerk(id),
    primary key (id)
);


