# sqlite created from the output of the ngamsPlotDataAccess.py
create table ac(ts integer, obs_id integer, offline integer, file_size integer, user_ip varchar(256), obs_date char(10));
create index ac_ts_index on ac(ts);
.separator ","
.import 2015-08-01T19-42-25-int.csv ac

# postgresql on MC database
\f ','
\a
\t
\o obsId_prjId.csv
select starttime, projectid from mwa_setting;

#sqlite
create table opm(obs_id integer, proj_id varchar(256));
create index opm_obs_id_idx on opm(obs_id);
.separator ","
.import obsId_prjId.csv opm

create table vis(obs_id integer, file_size integer);
create index vis_obs_id_idx on vis(obs_id);
.separator ","
.import all_vis.csv vis

# sqlite getting file distribution by project id (joined with NGAS table)
select sum(a.file_size) proj_size, b.proj_id from vis a, opm b where a.obs_id = b.obs_id group by b.proj_id order by proj_size desc;

# sqlite find out utilisation for each project
.mode csv
.header off
.out ingested_not_retrieved.csv

select distinct(a.obs_id) from
  (select obs_id from ac where offline = -1) a
left outer join
  (select obs_id from ac where offline <> -1) b
on b.obs_id = a.obs_id
where b.obs_id is NULL;

# import ingested and retrieved into the sqlite db
create table inr(obs_id integer);
create index inr_obs_id_idx on inr(obs_id);
.separator ","
.import ingested_not_retrieved.csv inr

# join with the project table

select a.proj_id, count(c.ac_obsid) total_count from opm a,
(select distinct(obs_id) as ac_obsid from ac where offline = -1) c
where a.obs_id = c.ac_obsid
group by a.proj_id
order by a.proj_id

select a.proj_id, count(b.obs_id) non_count from opm a, inr b
where a.obs_id = b.obs_id
group by a.proj_id
order by a.proj_id

select aa.proj_id, aa.total_count, ifnull(bb.non_count, 0) from
(select a.proj_id, count(c.ac_obsid) total_count from opm a,
(select distinct(obs_id) as ac_obsid from ac where offline = -1) c
where a.obs_id = c.ac_obsid
group by a.proj_id) aa
left join
(select a.proj_id, count(b.obs_id) non_count from opm a, inr b
where a.obs_id = b.obs_id
group by a.proj_id
order by a.proj_id) bb
on aa.proj_id = bb.proj_id
order by aa.total_count desc

# sqlite getting file distribution by project id (joined with access table)
select sum(a.file_size) proj_size, b.proj_id from ac a, opm b where a.offline = -1 and a.obs_id = b.obs_id group by b.proj_id order by proj_size desc;
-- 3917462398121714,G0002
-- 2621903780153924,G0008
-- 2551582246323487,G0009
-- 1236267796532545,D0000
-- 1200954293491640,D0006
-- 524492540432807,G0017
-- 502117345195490,D0005
-- 264753094426931,G0001
-- 192272428744420,G0016
-- 112323998382464,G0011
-- 100388953133129,C001
-- 72843740330936,D0008
-- 62934856782868,G0010
-- 61812093437431,G0004
-- 57514910077908,D0001
-- 56496832711066,G0020
-- 50803864581736,D0002
-- 41598459280967,G0018
-- 39071306359210,C100
-- 29255886284309,D0007
-- 24614486346972,C102
-- 18758726226728,G0012
-- 14728327594297,G0003
-- 13816560318318,A0001
-- 11422393561482,G0015
-- 9486966385548,G0023
-- 6513141731951,D0009
-- 6025527883682,D0004
-- 5506100511080,D0010
-- 3848379921449,G0024
-- 291829045368,G0021
-- 128215255272,G0019
-- 99214529130,OA002

# NGAS postgresql (archive volume by)
select substring(file_id, 1, 10), file_size from ngas_files where disk_id <> '848575aeeb7a8a6b5579069f2b72282c';
