-- Скрипт создания процедур заполнения данными витрины DM.DM_ACCOUNT_BALANCE_F

create or replace procedure ds.FillStart_DM_ACCOUNT_BALANCE_F(startDate date)
language plpgsql
as $$
begin
	call logs.PrintInfo('Начало расчёта витрины DM.DM_ACCOUNT_BALANCE_F за последний день предыдущего месяца');
	delete from dm.dm_account_balance_f where on_date = startDate;
	insert into dm.dm_account_balance_f 
	select bc.on_date, bc.account_rk, bc.balance_out, bc.balance_out * coalesce(ex.reduced_cource, 1) as balance_out_rub
	from( select b.currency_rk, on_date, account_rk, balance_out
	from ft_balance_f b
	inner join md_currency_d c on
			b.currency_rk = c.currency_rk
	where on_date = startDate
	and 
	startDate between c.data_actual_date and c.data_actual_end_date) bc
	left join 
	(
		select currency_rk, reduced_cource
	from md_exchange_rate_d
	where startDate between data_actual_date and data_actual_end_date)
		ex on
		bc.currency_rk = ex.currency_rk;
	call logs.PrintInfo('Конец расчёта витрины DM.DM_ACCOUNT_BALANCE_F за последний день предыдущего месяца');
	exception when others then
		call logs.PrintError('Ошибка расчёта витрины DM.DM_ACCOUNT_BALANCE_F за последний день предыдущего месяца');
		call logs.PrintError(sqlerrm);
end; $$;


create or replace procedure Fill_DM_ACCOUNT_BALANCE_F(startDate date, endDate date)
language plpgsql
as $$
declare
	calcDate date;
	count int;
begin
	call logs.PrintInfo('Начало расчёта витрины DM.DM_ACCOUNT_BALANCE_F');
	count = endDate - startDate;
    calcDate = startDate;
	for counter in 0..count loop
		delete from dm.dm_account_balance_f where on_date = calcDate;
		call fill_account_balance_f(calcDate);
		calcDate = calcDate + INTERVAL '1 day';
	end loop;
	call logs.PrintInfo('Начало расчёта витрины DM.DM_ACCOUNT_BALANCE_F');
	exception when others then
		call logs.PrintError('Ошибка расчёта витрины DM.DM_ACCOUNT_BALANCE_F');
		call logs.PrintError(sqlerrm);
end; $$;