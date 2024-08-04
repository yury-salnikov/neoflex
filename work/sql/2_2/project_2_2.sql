--DEAL_INFO
select * from rd.deal_info;
select count(*) from rd.deal_info di ;	                                 				--6500 1. Всего строк
select count(*) from (select distinct * from rd.deal_info di);           				--6500 2. Нет полных дубликатов 
select count(distinct deal_rk) from rd.deal_info di ;	                 				--6498 3. Есть дубликаты по потенциальному первичному ключу deal_rk
select count(distinct (deal_rk, effective_from_date)) from rd.deal_info; 				--6499 4. Есть  дубликаты по потенциальному первичному ключу deal_rk, effective_from_date
select count(distinct (deal_rk, product_rk, effective_from_date)) from rd.deal_info; 	--6500 5. НЕТ ДУБЛИКАТОВ ПО ПОТЕНЦИАЛЬНОМУ ПЕРВИЧНОМУ КЛЮЧУ DEAL_RK, PRODUCT_RK, EFFECTIVE_FROM_DATE
select count(distinct deal_num) from rd.deal_info di;	                 				--6500 6. Столбец с уникальными значениями, нужно бдует проверить после слияния на уникальность

select * from rd.deal_info di where deal_rk in (
select deal_rk from rd.deal_info di 
group by deal_rk 
having count(*) > 1
) order by deal_rk;																		--Проверка дубликатов по deal_rk

select * from rd.deal_info di where (deal_rk,effective_from_date) in (
select deal_rk, effective_from_date from rd.deal_info di 
group by deal_rk, effective_from_date
having count(*) > 1
) order by deal_rk, effective_from_date;												--Проверка дубликатов по deal_rk, effective_from_date

select * from rd.deal_info di  where deal_rk is null or product_rk is null or effective_from_date is null or effective_to_date  is null;	-- 7. Нет Null среди полей потенциального первичного ключа
	

--LOAN_HOLIDAY
select * from rd.loan_holiday;
select count(*) from rd.loan_holiday;													--10000 1. Всего строк
select count(*) from (select distinct * from rd.loan_holiday);							--10000 2. Нет полных дубликатов
select count(distinct deal_rk) from rd.loan_holiday;									--9995	3. Есть дубликаты по потенциальному первичному ключу deal_rk
select count(distinct (deal_rk, effective_from_date)) from rd.loan_holiday;				--9998  4. Есть дубликаты по потенциальному первичному ключу deal_rk, effective_from_date
select count(distinct (deal_rk, effective_from_date,
					loan_holiday_last_possible_date)) from rd.loan_holiday;				--10000 5. НЕТ ДУБЛИКАТОВ ПО ПОТЕНЦИАЛЬНОМУ ПЕРВИЧНОМУ КЛЮЧУ DEAL_RK, LOAN_HOLIDAY_LAST_POSSIBLE_DATE, EFFECTIVE_FROM_DATE

select * from rd.loan_holiday 
where deal_rk in 
(
	select deal_rk from rd.loan_holiday
	group by deal_rk
	having count(*) > 1
)
order by deal_rk, effective_from_date;													--Проверка дубликатов по deal_rk

select * from rd.loan_holiday 
where (deal_rk, effective_from_date) in 
(
	select deal_rk,effective_from_date from rd.loan_holiday
	group by deal_rk, effective_from_date 
	having count(*) > 1
)
order by deal_rk, effective_from_date;												    --Проверка дубликатов по deal_rk, effective_from_date

select * from rd.loan_holiday  where deal_rk is null or LOAN_HOLIDAY_LAST_POSSIBLE_DATE is null or effective_to_date  is null;	-- 6. Нет Null среди полей потенциального первичного ключа


--PRODUCT
select * from rd.product;
select count(*) from rd.product;														--3500 	1. Всего строк
select count(*) from (select distinct * from rd.product);								--3498  2. Есть полные дубликаты!
select count(distinct product_rk) from rd.product;  									--3496	3. Есть дубликаты по потенциальному первичному ключу product_rk
select count(distinct (product_rk, effective_from_date)) from rd.product; 				--3496	4. Есть дубликаты по потенциальному первичному ключу product_rk, effective_from_date
select count(distinct (product_rk, product_name, effective_from_date)) from rd.product; --3498  5. НЕТ ДУБЛИКАТОВ ПО ПОТЕНЦИАЛЬНОМУ ПЕРВИЧНОМУ КЛЮЧУ PRODUCT_RK, PRODUCT_NAME, EFFECTIVE_FROM_DATE

select * from rd.product where product_rk in (
select product_rk from (select distinct * from rd.product)
group by product_rk 
having count(*) > 1
)order by product_rk;																	--Проверка дубликатов по product_rk

select * from rd.product where (product_rk, effective_from_date) in (
select product_rk, effective_from_date from (select distinct * from rd.product)
group by product_rk, effective_from_date
having count(*) > 1
)order by product_rk, effective_from_date;												--Проверка дубликатов по product_rk, effective_from_date

select * from rd.product where (product_rk, product_name, effective_from_date) in (
select product_rk, product_name, effective_from_date from (select distinct * from rd.product) 
group by product_rk, product_name, effective_from_date
having count(*) > 1
)order by product_rk, product_name, effective_from_date;								--Проверка дубликатов по product_rk, product_name, effective_from_date

select * from rd.product p where product_rk is null or product_name  is null or effective_from_date is null;	-- 6. Нет Null среди полей потенциального первичного ключа


--1979096 Проверка соединения продукта product_rk=1979096
select * from rd.product p where product_rk =1979096;
select * from rd.deal_info di where product_rk =1979096;
select * from rd.deal_info di left join rd.product p on di.product_rk=p.product_rk and di.effective_from_date =p.effective_from_date
where di.product_rk = 1979096;
select * from rd.deal_info di where product_rk =1979096;
select * from rd.deal_info di left join rd.product p on di.product_rk=p.product_rk and di.effective_from_date =p.effective_from_date and deal_name = p.product_name 
where di.product_rk = 1979096;

--1979096 Проверка соединения сделка deal_rk=2594431
select * from rd.deal_info di where deal_rk = 2594431;
select * from rd.deal_info di left join rd.product p on di.product_rk=p.product_rk and di.effective_from_date =p.effective_from_date
where di.deal_rk = 2594431;
--2594431 Проверка соединения сделка deal_rk=2594431
select * from rd.deal_info di where deal_rk = 2594431;
select * from rd.deal_info di left join rd.product p on di.product_rk=p.product_rk and di.effective_from_date =p.effective_from_date and deal_name = p.product_name 
where di.deal_rk = 2594431;






--CSV DEAL_INFO 3500 записией в CSV
select * from rd2.deal_info;
select count(*) from rd2.deal_info di ;													--3500 1. Всего строк в базе													
select count(*) from (select distinct * from rd2.deal_info di); 						--3500 2. Дубликаты удалены
select count(distinct deal_rk) from rd2.deal_info di ;	                 				--3499 3. Есть дубликаты по потенциальному первичному ключу deal_rk
select count(distinct (deal_rk, effective_from_date)) from rd2.deal_info; 				--3499 4. Есть  дубликаты по потенциальному первичному ключу deal_rk, effective_from_date
select count(distinct (deal_rk, product_rk, effective_from_date)) from rd2.deal_info; 	--3500 5. НЕТ ДУБЛИКАТОВ ПО ПОТЕНЦИАЛЬНОМУ ПЕРВИЧНОМУ КЛЮЧУ DEAL_RK, PRODUCT_RK, EFFECTIVE_FROM_DATE

select * from rd2.deal_info di where deal_rk in (
select deal_rk from rd2.deal_info di 
group by deal_rk 
having count(*) > 1
) order by deal_rk;																		--Проверка дубликатов по deal_rk
select * from rd2.deal_info di  where deal_rk is null or effective_from_date is null or effective_to_date  is null;	-- 6. Нет Null среди полей потенциального первичного ключа


--CSV PRODUCT 10000 записей в CSV
select * from rd2.product;
select count(*) from rd2.product;														--9996 1. Всего строк в базе, 4 дубликата были удалены при загрузке из csv
select count(*) from (select distinct * from rd2.product);								--9996 2. Полных дубликатов нет
select count(distinct product_rk) from rd2.product;  									--9955 3. Есть дубликаты по потенциальному первичному ключу deal_rk
select count(distinct (product_rk, product_name, effective_from_date)) from rd2.product;--3498  5. НЕТ ДУБЛИКАТОВ ПО ПОТЕНЦИАЛЬНОМУ ПЕРВИЧНОМУ КЛЮЧУ PRODUCT_RK, PRODUCT_NAME, EFFECTIVE_FROM_DATE
select * from rd2.product p where product_rk is null or product_name  is null or effective_from_date is null or effective_to_date  is null;	-- 6. Нет Null среди полей потенциального первичного ключа


--ВИТРИНА
select * from dm.loan_holiday_info;
select count(*) from dm.loan_holiday_info;	--10002 1. Не знаю зачем мне эта информция т.к. непонятно по каким исходным таблицам строилась витрина возможно она для проверки завтра узнаем
SELECT count(*) FROM dm.loan_holiday_info;  --10032 после актуализации данных из csv файлов возможно лишние 30 строк надо сравнить строки витрины будет
select count(*) from (select distinct * from dm.loan_holiday_info lhi);
select count(*) from (select distinct * from dm2.loan_holiday_info lhi);
select count(*) from (select distinct deal_number from dm.loan_holiday_info lhi);
select count(*) from (select distinct deal_number from dm2.loan_holiday_info lhi);

select distinct deal_rk, product_rk from dm.loan_holiday_info lhi 
except
select distinct deal_rk, product_rk from dm2.loan_holiday_info lhi2;

select * from dm2.loan_holiday_info lhi where deal_number  is null;


-- После проверки оригинальной витрины оказалось, что строк должно быть больше чем 10002
-- Но если подгрузить CSV файлы и перестроить витрину и будет больше чем 10002 то заказчик будет доволен
-- ХОД ДЕЙСТВИЙ
-- 1. В исходный прототип добавил вывод account_rk т.к. он был в витрине
-- 2. До удаления дубликатов из исходных products прототип считает 6502 записи
-- 3. После удаления дубликатов из исходной products прототип считает 6502 записи


-- ОПРЕДЕЛИТЬ, ПО КАКИМ ДАТАМ ЭФФЕКТИВНОСТИ (EFFECTIVE_FROM_DATE ИЛИ EFFECTIVE_TO_DATE) ОТСУТСТВУЮТ СТРОКИ В ВИТРИНЕ
select distinct effective_from_date, effective_to_date from dm.loan_holiday_info;	--2023-01-01  2023-03-15  2023-08-11 ---------- 2999-12-31   Витрина исходная
select distinct effective_from_date, effective_to_date from dm.loan_holiday_info;	--2023-01-01              2023-08-11 ---------- 2999-12-31   Витрина перестроенная по исходным данным нет 2023-03-15
select distinct effective_from_date, effective_to_date from rd.loan_holiday;		--2023-01-01  2023-03-15  2023-08-11 ---------- 2999-12-31   Исходные данные нет актуальных CSV

select distinct effective_from_date, effective_to_date from rd.deal_info;		    --2023-01-01              2023-08-11 ---------- 2999-12-31   Исходные данные
select distinct effective_from_date, effective_to_date from rd2.deal_info;			--            2023-03-15			 ---------- 2999-12-31   csv
--Метод загрузки - загрузка части данных - интервалы не пересекаются кол-во строк будет 6500+3500=10000

select distinct effective_from_date, effective_to_date from rd.product;				--            2023-03-15			 ---------- 2999-12-31   Исходные данные
select distinct effective_from_date, effective_to_date from rd2.product;			--2023-01-01  2023-03-15  2023-08-11 ---------- 2999-12-31   csv
--Метод загрузки - полная загрузка или обновление интервалы дат пересекаются


with
a as(
select * from rd.product p where effective_from_date = '2023-03-15'),
b as (
select * from rd2.product p where effective_from_date ='2023-03-15')
select count(*) from (
select * from b
except
select * from a);	--Данные за 2023-03-15 дублируются в исходной product и csv, значит можно полную загрузку с очисткой исходной таблицы т.е. в новой актуальной таблице будет 9996 строк
