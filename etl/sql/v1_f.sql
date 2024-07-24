-- Скрипт создания процедуры заполнения данными витрины DM.DM_ACCOUNT_TURNOVER_F

create or replace procedure ds.Fill_DM_ACCOUNT_TURNOVER_F(startDate date, endDate date)
language plpgsql
as $$
declare
	calcDate date;
	count int;
begin
	call logs.PrintInfo('Начало расчёта витрины DM.DM_ACCOUNT_TURNOVER_F');
	count = endDate - startDate;
	calcDate = startDate;
	for counter in 0..count loop
		delete from dm.dm_account_turnover_f where on_date = calcDate;
		call fill_account_turnover_f(calcDate);
		calcDate = calcDate + INTERVAL '1 day';
	end loop;
	call logs.PrintInfo('Конец расчёта витрины DM.DM_ACCOUNT_TURNOVER_F');
	exception when others then 
		call logs.PrintError('Ошибка расчёта витрины DM.DM_ACCOUNT_TURNOVER_F');
		call logs.PrintError(sqlerrm);
end; $$;