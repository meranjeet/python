select txn_id--,count(cnt)
from
(select txn_id,case when in_time::date in ('2020-06-29','2020-07-01') then 'Y' else NULL end as cnt
from parking_lot) a
group by 1
having count(cnt)=2
--order by 1
UNION
select txn_id--,count(cnt)
from
(select txn_id,case when out_time::date in ('2020-06-29','2020-07-01') then 'Y' else NULL end as cnt
from parking_lot) a
group by 1
having count(cnt)=2
--order by 1
