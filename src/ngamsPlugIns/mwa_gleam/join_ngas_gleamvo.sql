create table gleamvo(obs_date char(10), center_freq integer, stokes integer, file_id varchar(256));
create index gleamvo_file_id on gleamvo(file_id);
.separator ","
.import gleam_vo.csv gleamvo


create table gleamfile(file_id varchar(256), file_path varchar(256));
create index gleamfile_file_id on gleamfile(file_id);
.separator ","
.import gleam_file.csv gleamfile

.mode csv
.header off
.out gleam_vofile_join.csv
select a.obs_date, a.center_freq, a.stokes, a.file_id, b.file_path from gleamvo a, gleamfile b
where a.file_id = b.file_id
order by a.obs_date, a.center_freq, a.stokes, a.file_id;

create table gleamvofile(obs_date char(10), center_freq integer, stokes integer, file_id varchar(256), file_path varchar(256));
.separator ","
.import gleam_vofile_join.csv gleamvofile


