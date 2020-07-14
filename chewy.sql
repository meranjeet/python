/* Repeat customer 
select txn_id
from parking_lot
group by txn_id
having count(*)>1
order by 1
*/
select txn_id
from
(
select txn_id,fdate,row_number() over (partition by txn_id order by fdate) as rnbr,
	fdate::date - cast((row_number() over (partition by txn_id order by fdate)) as integer)*30 as cnt
from
(
select txn_id,date_trunc('month',in_time) as fdate,row_number() over (partition by txn_id,date_trunc('month',in_time) order by null ) as rn
from parking_lot
) a
where rn=1
	) b
group by txn_id
having count(cnt)>=2
order by 1
