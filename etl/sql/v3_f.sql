-- Скрипт создания процедуры заполнения данными витрины формы F101 DM.DM_F101_ROUND_F
create or replace procedure ds.Fill_DM_F101_ROUND_F(onDate date)
language plpgsql
as $$
begin 
	call logs.PrintInfo('Начало расчёта витрины формы F101');
	call fill_f101_round_f(onDate);
	call logs.PrintInfo('Конец расчёта витрины формы F101');
	exception when others then
		call logs.PrintError('Ошибка расчёта формы F101');
		call logs.PrintError(sqlerrm);
end; $$;
