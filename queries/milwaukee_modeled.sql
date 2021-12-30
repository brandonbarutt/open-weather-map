create or replace view milwaukee_modeled as 

select *,
	   (rain/25.4) as rain_inches,
	   round((snow/25.4)*
	   (case when temp >= 28 then 10
	   		 when temp >= 20 then 15
	   		 when temp >= 15 then 20
	   		 when temp >= 10 then 30
	   		 when temp >= 0 then 40
	   		 when temp >= -20 then 50
	   	     when temp >= -40 then 100
	   		 else 0 end),2) as snow_inches 
from milwaukee_raw