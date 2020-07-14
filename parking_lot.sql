--create table parking_lot(txn_id varchar(20), in_time timestamp, out_time timestamp);

select * from parking_lot where '2020-06-29 12:00:00' between in_time and out_time;

--drop table parking_lot