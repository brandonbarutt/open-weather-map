with rw as 

(

select *, 
	   case when month(dt) in (1,2) then year(dt) - 1 else year(dt) end as year,
	   case when month(dt) in (12,1,2) then 'winter'
	   	    when month(dt) in (3,4,5) then 'spring'
	   	    when month(dt) in (6,7,8) then 'summer'
	   	    when month(dt) in (9,10,11) then 'fall'
	   	    else 'error' end as season
from milwaukee_modeled mm 
where date(dt) between '1980-01-01' and current_date() and 
	  city = 'Milwaukee'

),

aggregated as 

(

select year,
	   season,
	   date(dt) as date,
	   sum(rain_inches) as rain_inches,
	   sum(snow_inches) as snow_inches,
	   avg(temp) as temp
from rw
group by year, season, date(dt)

),

slim as 

(

select date,
	   temp,
	   year,
	   season,
	   rain_inches,
	   snow_inches,
	   row_number() over (partition by year, season order by date) as day_no
from aggregated
order by date

),

pre_final as 

(

select season,
	   year,
	   day_no,
	   sum(rain_inches) over (partition by year, season order by day_no) as cumulative_rain_inches,
	   sum(snow_inches) over (partition by year, season order by day_no) as cumulative_snow_inches,
	   avg(temp) over (partition by year, season order by day_no) as average_temperature
from slim
order by year, season, day_no

),

final_tmp as 

(

select season,
	   year,
	   day_no,
	   case when year between 1981 and 1990 then '1981-1990'
	  		when year between 1991 and 2000 then '1991-2000'
	  		when year between 2001 and 2010 then '2001-2010'
	  		when year between 2011 and 2020 then '2011-2020'
	  		when year = 2021 then '2021'
	  		when year = 2022 then '2022'
	  		else 'other' end as time_period,	  		
	   round(cumulative_rain_inches,2) as cumulative_rain_inches,
	   round(cumulative_snow_inches,2) as cumulative_snow_inches,
	   round(average_temperature,2) as average_temperature
from pre_final
order by year, season, day_no

)

select time_period,
	   season,
	   day_no,
	   count(*) as distinct_years,
	   avg(cumulative_rain_inches) as cumulative_rain_inches,
	   avg(cumulative_snow_inches) as cumulative_snow_inches,
	   avg(average_temperature) as average_temperature
from final_tmp
group by time_period, season, day_no
order by time_period, season, day_no;