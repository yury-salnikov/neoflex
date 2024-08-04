create or replace procedure rd.delete_product_duplicates()
language plpgsql
as $$
begin
	drop table if exists rd.product_temp;
	create table rd.product_temp (like rd.product);
	insert into rd.product_temp select distinct * from rd.product;
	drop table rd.product;
	alter table rd.product_temp rename to product;
end; $$;


create or replace procedure rd.delete_loan_holidays_duplicates()
language plpgsql
as $$
begin
	delete from rd.loan_holiday
	where (deal_rk, loan_holiday_last_possible_date, effective_from_date) in 
	(
		select a.deal_rk, a.loan_holiday_last_possible_date, a.effective_from_date  from 
		(select * from rd.loan_holiday) a inner join (
		select deal_rk, max(loan_holiday_last_possible_date) last_date, effective_from_date from rd.loan_holiday
		group by deal_rk, effective_from_date
		having count(*) > 1) b on a.deal_rk = b.deal_rk and a.effective_from_date = b.effective_from_date
		where a.loan_holiday_last_possible_date< b.last_date
	);
end; $$;


call rd.delete_product_duplicates();
call rd.delete_loan_holidays_duplicates();