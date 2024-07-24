-- Полная очистка БД от объектов ETL
drop schema if exists ds, dm, logs cascade;

-- Заполнение витрины оборотов за январь 2018
call Fill_DM_ACCOUNT_TURNOVER_F('2018-01-01', '2018-01-31');

-- Заполнение витрины остатков за январь 2018
call FillStart_DM_ACCOUNT_BALANCE_F('2017-12-31');
call Fill_DM_ACCOUNT_BALANCE_F('2018-01-01', '2018-01-31');

-- Заполнение витрины формы F101 за январь 2018
call Fill_DM_F101_ROUND_F('2018-02-01');