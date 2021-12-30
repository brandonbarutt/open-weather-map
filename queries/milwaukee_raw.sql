create or replace view milwaukee_raw as 

select hd.city_name as city,
	   convert_tz(cast(hd.dt_iso as datetime),'UTC','America/Chicago') as dt,
	   hd.temp as temp,
	   hd.feels_like as feels_like,
	   hd.pressure as pressure,
	   hd.humidity as humidity,
	   hd.dew_point as dew_point,
	   hd.clouds_all as clouds,
	   hd.visibility as visibility,
	   hd.wind_speed as wind_speed,
	   hd.wind_deg as wind_deg,
	   hd.wind_gust as wind_gust,
	   coalesce(hd.rain_1h,0) as rain,
	   coalesce(hd.snow_1h,0) as snow
from historical_data hd 
where hd.dt_iso < '2021-12-17 00:00:00'

union all

select du.city,
	   convert_tz(cast(du.dt as datetime),'UTC','America/Chicago') as dt,
	   du.temp,
	   du.feels_like,
	   du.pressure,
	   du.humidity,
	   du.dew_point,
	   du.clouds,
	   du.visibility,
	   du.wind_speed,
	   du.wind_deg,
	   du.wind_gust,
	   coalesce(du.rain,0) as rain,
	   coalesce(du.snow,0) as snow
from daily_upload du
where du.city = 'Milwaukee'
