-- Скрипт создания и тестирования процедуры заполнения витрины оборотов DM.DM_ACCOUNT_TURNOVER_F

create or replace procedure ds.fill_account_turnover_f(i_OnDate date)
language plpgsql
as $$
begin
	insert into dm.dm_account_turnover_f
		select o.on_date, o.account_rk, o.credit_amount, o.credit_amount*coalesce(ex.cource, 1) as credit_amount_rub, 
		o.debet_amount, o.debet_amount*coalesce(ex.cource, 1) as debet_amount_rub
		from 
		(
			select coalesce(ct.oper_date, dt.oper_date) as on_date,
			coalesce(ct.credit_account_rk, dt.debet_account_rk) as account_rk,
			coalesce(ct.credit_amount, 0) as credit_amount,
			coalesce (dt.debet_amount, 0) as debet_amount
			from 
			(
			select oper_date, credit_account_rk, sum(credit_amount) as credit_amount  from ft_posting_f fpf 
			group by oper_date, credit_account_rk 
			having oper_date = i_OnDate
			) ct full join
			(
			select oper_date, debet_account_rk, sum(debet_amount) as debet_amount  from ft_posting_f fpf 
			group by oper_date, debet_account_rk 
			having oper_date = i_OnDate
			) dt
			on ct.credit_account_rk = dt.debet_account_rk
		) o inner join 
		(
			select a.account_rk, a.currency_rk from md_account_d a 
				inner join md_currency_d c on a.currency_rk = c.currency_rk
			where i_OnDate between a.data_actual_date and a.data_actual_end_date and 
				  i_OnDate between c.data_actual_date and c.data_actual_end_date 
		) acc on o.account_rk = acc.account_rk
		left join
		(
			select currency_rk, reduced_cource as cource from md_exchange_rate_d where i_OnDate between data_actual_date and data_actual_end_date
		) ex on acc.currency_rk = ex.currency_rk;
end; $$;
