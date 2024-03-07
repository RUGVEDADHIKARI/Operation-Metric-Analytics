create database project31;
use project31;
### table-1 user
create table table31(
	user_id int,
    created_at varchar(50),
    company_id int,
    language varchar(50),
    activated_at varchar(100),
    state varchar(50)
);

show variables like "secure_file_priv";

LOAD DATA INFILE "users.csv"
INTO TABLE table31
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from table31;
alter table table31 rename users;
select * from users;
alter table users add column temp_created datetime;
alter table users add column id int;
select * from users;
SET SQL_SAFE_UPDATES = 0;
UPDATE users 
SET temp_created = STR_TO_DATE(created_at, "%d-%m-%Y %H:%i");
alter table users drop column created_at;
alter table users change column temp_created created_at datetime;
select *from users;

## table-2
create table events(
	user_id int null,
    occured_at varchar(100) null,
    event_type varchar(50) null,
    event_name varchar(50) null,
    location varchar(50) null,
    device varchar(50) null,
    user_type int null
    );
LOAD DATA INFILE "events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from events;
alter table events add column temp_created datetime;
SET SQL_SAFE_UPDATES = 0;
UPDATE events 
SET temp_created = STR_TO_DATE(occured_at, "%d-%m-%Y %H:%i");
alter table events drop column occured_at;
alter table events change column temp_created occured_at datetime;
select * from events;

create table emailEvents(
	user_id int,
    occured_at varchar(50),
    action varchar(100),
    user_type int
);
LOAD DATA INFILE "email_events.csv"
INTO TABLE emailEvents
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from emailEvents;
alter table emailEvents add column temp_created datetime;
SET SQL_SAFE_UPDATES = 0;
UPDATE emailEvents 
SET temp_created = STR_TO_DATE(occured_at, "%d-%m-%Y %H:%i");
alter table emailEvents drop column occured_at;
alter table emailEvents change column temp_created occured_at datetime;
select * from events;

##Weekly User Engagement
select * from users;
SELECT extract(week from created_at) as weekly_active,count(distinct user_id) as users from users group by weekly_active;






##User Growth Analysis
select * from users;
select year,week_num,users,sum(users) over (order by year,week_num) as cum_users
from( select extract(year from created_at) as year,extract(week from created_at) as week_num,count(distinct user_id) as users
from users group by year,week_num order by year,week_num
)sub;


##Weekly Retention Analysis
select * from events;
select distinct user_id,extract(week from occured_at) as signup_week from events;
with base1 as(
select distinct user_id,extract(week from occured_at) as signup_week from events 
where event_type="signup_flow" and event_name="complete_signup" and extract(week from occured_at)=18),
base2 as (select distinct user_id,extract(week from occured_at) as engagement_week from events
where event_type="engagement")
select count(user_id) as total_engaged_users,sum(case when retention_week > 8 then 1 else 0 end) as retained_users
from(select a.user_id,a.signup_week,b.engagement_week-a.signup_week as retention_week from base1 a 
left join base2 b on a.user_id=b.user_id order by a.user_id) sub;

##weekly engagement per device
select * from events;
WITH base1 AS (
SELECT user_id AS users,EXTRACT(YEAR FROM occured_at) AS year_use,EXTRACT(WEEK FROM occured_at) AS weeknum,device
FROM events WHERE event_type = "engagement" GROUP BY users, year_use, weeknum, device  ORDER BY weeknum
)
SELECT users,year_use,weeknum,device FROM base1;

##Email Engagement Analysis
select * from emailEvents;
select 100* sum(case when email="email_open" then 1 else 0 end)/sum(case when email="email_sent" then 1 else 0 end) as email_open_rate,
100* sum(case when email="email_clicked" then 1 else 0 end)/sum(case when email="email_sent" then 1 else 0 end) as email_clicked_rate
from(select *, case when action in ("sent_weekly_digest","sent_reenagagement_email") then "email_sent"
when action in ("email_open") then "email_open" when action in ("email_clickthrough") then "email_clicked"
end as email
from emailEvents) sub; 


