create table daily_upload (
id int primary key auto_increment,
city varchar(20),
dt datetime,
temp decimal(5,2),
feels_like decimal(5,2),
pressure smallint,
humidity tinyint,
dew_point decimal(5,2),
uvi decimal(5,2),
clouds tinyint,
visibility mediumint,
wind_speed decimal(5,2),
wind_deg smallint,
wind_gust decimal(5,2),
rain decimal(5,2),
snow decimal(5,2)
)