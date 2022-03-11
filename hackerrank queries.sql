select c.algorithm,
sum( CASE WHEN QUARTER((cast(t.dt as date))) = 1 and year((cast(t.dt as date))) = 2020  THEN t.volume ELSE 0 END) AS Q1,
sum( CASE WHEN QUARTER((cast(t.dt as date))) = 2 and year((cast(t.dt as date))) = 2020 THEN t.volume ELSE 0 END) AS Q2,
sum( CASE WHEN QUARTER((cast(t.dt as date))) = 3 and year((cast(t.dt as date))) = 2020 THEN t.volume ELSE 0 END) AS Q3,
sum( CASE WHEN QUARTER((cast(t.dt as date))) = 4 and year((cast(t.dt as date))) = 2020 THEN t.volume ELSE 0 END) AS Q4
 from coins as c 
inner join transactions as t
on c.code = t.coin_code
group by c.algorithm
order by c.algorithm

-- hours worked on weekends
with t1 as (Select emp_id, CAST(timestamp as date) as dt, CAST(timestamp as time)	as tm, timestamp
from attendance),
t2 as (
		select *, (CASE WHEN (((DATEPART(DW, dt) - 1 ) + @@DATEFIRST ) % 7) IN (0,6) 
			THEN 1 ELSE 0 END) AS is_weekend_day from t1),
t3 as (select * from t2  where is_weekend_day = 1),

t4 as (select emp_id, min(tm) as clock_in , max(tm) as clock_out  from t3
group by dt, emp_id),
t5 as (select * ,DATEDIFF(hour,clock_in, clock_out ) as diff from t4)
select emp_id , sum(diff) from t5
group by emp_id


with t1 as (Select emp_id, CAST(timestamp as date) as dt, CAST(timestamp as time)    as tm, timestamp
from attendance),
t2 as (
        select *, (CASE WHEN weekday(dt) IN (5,6) 
            THEN 1 ELSE 0 END) AS is_weekend_day from t1),
t3 as (select * from t2  where is_weekend_day = 1),
t4 as (select emp_id, min(tm) as clock_in , max(tm) as clock_out  from t3
group by dt, emp_id),
t5 as (select * ,hour(timeDIFF(clock_out,clock_in)) as diff from t4)
select emp_id , sum(diff) from t5
group by emp_id



hour(timeDIFF('2000-01-01 10:45:00','2000-01-01 00:46:00'))





--601. Human Traffic of Stadium
# Write your MySQL query statement below
with t1 as (
select * , 
row_number() over(order by id) as rn,
id - (row_number() over (order by id)) as difference
from stadium
where people >= 100),
t2 as 
    (select *, count(*) over(partition by difference ) as no_records
     from t1)
select  id, visit_date, people from t2
where no_records >= 3

-- wheather analysis 
 with t1 as (select month((cast(record_date as date))) as month_avg,
        CASE WHEN data_type = 'max' THEN data_value else 0  END AS MAX_VALUE,
        CASE WHEN data_type = 'min' THEN data_value else null  END AS min_VALUE,
        CASE WHEN data_type = 'avg' THEN data_value else null  END AS avg_VALUE
from temperature_records)
select month_avg,  max(MAX_VALUE), min(min_VALUE), round(avg(avg_VALUE),0)  from t1
group by month_avg
        
        



select emp_id, cast(timestamp as date) as dt, 
       cast(timestamp as time) as tm, 
       row_number() as rn     
from attendance

    


--- events rank
with t1 as (select participant_name, score, event_id,
row_number() over ( Partition by participant_name order by cast(score as float) desc ) as rn
from scoretable$),
t2 as (select * from t1
where rn = 1),
t3 as (select *, DENSE_RANK() over (partition by event_id order by score desc) as rank  
from t2),
t4 as (select event_id, 
	CASE WHEN rank = 1 THEN participant_name else null  END AS first,
	CASE WHEN rank = 2 THEN participant_name else null  END AS second,
	CASE WHEN rank = 3 THEN participant_name else null  END AS third
from t3)
select event_id, STRING_AGG (first, ',') as first, STRING_AGG (second, ',') second ,STRING_AGG (third, ',') third   from t4
group by 
event_id

with t1 as (select participant_name, score, event_id,
row_number() over ( Partition by participant_name order by cast(score as float) desc ) as rn
from scoretable),
t2 as (select * from t1 where rn = 1),
t3 as (select *, DENSE_RANK() over (partition by event_id order by cast(score as float) desc) as r  
from t2),
t4 as (select event_id, 
    CASE WHEN r = 1 THEN participant_name else null  END AS first,
    CASE WHEN r = 2 THEN participant_name else null  END AS second,
    CASE WHEN r = 3 THEN participant_name else null  END AS third
from t3)
select event_id, GROUP_CONCAT(first ORDER BY first asc) as first, GROUP_CONCAT(second ORDER BY second asc) as second, GROUP_CONCAT(third ORDER BY third asc) as third  from t4
group by 
event_id




