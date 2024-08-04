create or replace procedure dm.delete_client_duplicates()
language plpgsql
as $$
begin
	--1. Сначала устраним полное дублирование
	drop table if exists dm.client_temp;
	create table dm.client_temp (like dm.client);
	insert into dm.client_temp select distinct * from dm.client;
	drop table dm.client;
	alter table dm.client_temp rename to client;
	--2. Затем будем устранять дублирование по первичному ключу
	--2.1 Для устранения дублирования будем удалять записи с наименьшим значением effective_to_date
	delete
	from client
	where (client_rk, effective_from_date, effective_to_date) in (
			select client_rk, effective_from_date, effective_to_date
			from(
				select row_number() over(partition by client_rk, effective_from_date order by effective_to_date desc) as rn,
					client_rk, effective_from_date, effective_to_date
				from client
			) t
			where t.rn > 1
			);
end; $$


call dm.delete_client_duplicates();

select * from dm.client c ;	 --20147
select count(*) from (select distinct * from dm.client c); --10020
select count(*) from (select distinct client_rk, effective_from_date from dm.client c); --10019
select distinct * from dm.client where (client_rk, effective_from_date) in 				--Дублирование по первичному ключу. Для устранения дублирования будем удалять записи с наименьшим значением effective_to_date.
(
select client_rk, effective_from_date from (select distinct * from dm.client c)
group by client_rk, effective_from_date
having count(*) > 1
);	--3055149
select * from dm.client c where client_rk = 3055149 and effective_from_date ='2023-08-11';