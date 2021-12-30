delete t1 from daily_upload t1
inner join daily_upload t2 
where 
    t1.id < t2.id and 
    t1.city = t2.city and 
    t1.dt = t2.dt