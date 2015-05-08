\f ','
\a
\t
\o gleam_vo.csv
select date_obs, center_freq, stokes, filename from mwa.gleam 
where band_width = 7.72
and (string_to_array((string_to_array(filename, '_r'))[2], '_'))[1] = '-1.0' 
order by date_obs, center_freq, stokes, filename;