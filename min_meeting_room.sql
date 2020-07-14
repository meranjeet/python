select max(cnt)
from
(select a.id,count(*) as cnt
from tm a,
tm b
where a.srt between b.srt and b.ed
group by 1) a