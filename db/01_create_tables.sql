set session authorization geo_ov;

create schema data;

alter database kv7netwerk set search_path=data,public;

set search_path=data,public;

create table imports(
    id serial,
    recv_date timestamp not null,
    file varchar not null,
    file_md5 varchar,
    state varchar not null,
    state_date timestamp not null,
    data_owner varchar,
    data_date timestamp,
    schema varchar,
    log text,
    extent box3d_extent,
    primary key (id));

