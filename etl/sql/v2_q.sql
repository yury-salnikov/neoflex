-- Скрипт создания процедуры заполнения витрины остатков DM.DM_ACCOUNT_BALANCE_F

create or replace procedure ds.fill_account_balance_f(i_OnDate date)
language plpgsql
as $$
declare
prevDate date;
begin
	prevDate = i_OnDate - INTERVAL '1 day';
	insert into dm.dm_account_balance_f
		select
		i_OnDate as on_date,
		acc.account_rk, 
		case
			when char_type = 'А' then 
				coalesce(balance_out,0) + coalesce(debet_amount,0) - coalesce (credit_amount, 0)
			when char_type = 'П' then 
				coalesce(balance_out,0) - coalesce(debet_amount,0) + coalesce (credit_amount, 0)
			else
			0
		end as balance_out,
		case
			when char_type = 'А' then 
				coalesce(balance_out_rub,0) + coalesce(debet_amount_rub,0) - coalesce (credit_amount_rub, 0)
			when char_type = 'П' then 
				coalesce(balance_out_rub,0) - coalesce(debet_amount_rub,0) + coalesce (credit_amount_rub, 0)
			else
			0
		end as balance_out_rub
		from
		(select account_rk, char_type from md_account_d acc
		where i_OnDate between data_actual_date and data_actual_end_date) acc 
		left join 
		(select account_rk, balance_out, balance_out_rub from dm.dm_account_balance_f where on_date = prevDate) rest
		on acc.account_rk = rest.account_rk
		left join 
		(select account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub from dm.dm_account_turnover_f where on_date = i_OnDate) ob
		on acc.account_rk = ob.account_rk;
end; $$;