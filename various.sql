--create table duplicate(name varchar(30))

---insert into duplicate values('Ranjeet')
--insert into duplicate values('Manjeet')


--delete from duplicate where ctid not in (select max(ctid) from duplicate group by name)
--create table csum(a integer, amt integer)

--insert into csum values(1,100);
--insert into csum values(2,300);
--insert into csum values(4,100);
--insert into csum values(5,500);
--insert into csum values(6,200);


--create table dup_del(c1 varchar(4),c2 varchar(4), amt integer)
-- insert into dup_del values('A','B',100);
-- insert into dup_del values('A','B',200);
--insert into dup_del values('B','A',400);
-- insert into dup_del values('C','D',400);
-- insert into dup_del values('D','E',500);



-- delete from dup_del
-- where ctid in (select ctid from )

-- select * from dup_del
-- where ctid not in (select max(ctid)  from dup_del
-- where concat(c1,c2) in ('AB','BA') )
-- and concat(c1,c2) in ('AB','BA')
--IND, PAK, CHN, AFG, SRI, BN

--create table cntry(cntry varchar(10))

-- insert into cntry values('IND');
-- insert into cntry values('PAK');
-- insert into cntry values('AFG');
-- insert into cntry values('SRI');
-- insert into cntry values('BAN');

--select c1.cntry,c2.cntry from cntry c1,cntry c2 where c1.cntry>c2.cntry order by 1

-- create table t(d integer);
-- insert into t values(1);
-- insert into t values(2);
-- insert into t values(3);
-- insert into t values(4);
-- insert into t values(5);
-- insert into t values(6);
-- insert into t values(7);
-- insert into t values(8);
-- insert into t values(9);
-- insert into t values(10);
-- create table bank(d integer, bal integer);
-- insert into bank values(2, 300);
-- insert into bank values(5, 700);
-- insert into bank values(8, -100);

--select t.d,sum(coalesce(bank.bal,0)) over (order by t.d asc) nbal from t left join bank on t.d=bank.d




-- --drop table stk
-- create table stk(store varchar(10), dt integer,unit integer);
-- insert into stk values('A1',1,0);
-- insert into stk values('A1',2,0);
-- insert into stk values('A1',3,0);
-- insert into stk values('A1',4,0);
-- insert into stk values('A1',5,1);
-- insert into stk values('A1',6,1);
-- insert into stk values('A1',7,1);
-- insert into stk values('A1',8,0);
-- select store
--       ,dt as srt
-- 	  ,edt
-- 	  ,unit 
-- from (
--       select *
-- 	         ,row_number() over (order by dt) as rn
-- 	         ,lead(dt) over () as edt 
-- 	  from (
--              select *
-- 		            ,lag(unit) over (order by dt) as ounit
-- 		            ,lead(unit) over (order by dt) as nunit
-- 	         from stk
-- 	        ) a
--       where unit<>coalesce(ounit,-1) or unit<>coalesce(nunit,-1)
-- 	   ) b
-- where mod(rn,2)!=0

-- create table A(id integer);
-- create table B(id integer);
-- insert into A values(1);
-- insert into A values(1);
-- insert into A values(1);
-- insert into A values(1);
-- insert into A values(1);
-- insert into B values(1);
-- insert into B values(1);

select count(*)
from A,B 

