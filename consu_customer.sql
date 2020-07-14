select customer_id,c,min(rn),c+cast(min(rn) as integer),count(*)
from
(
select customer_id,pdate,coalesce(pdate,'1900-01-10')-cast(rn as integer) as c,rn
from
(
select customer_id,pdate,
row_number() over (partition by customer_id order by pdate asc) as rn
--date(payment_date)-1
--datediff(date(payment_date),row_number() over (partition by customer_id order by date(payment_date) asc)) as diff
from (select distinct customer_id,date(payment_date) as pdate from payment) a 
	)b where customer_id=2
) c
group by 1,2
having count(*)>3
order by 1
	
	
	
