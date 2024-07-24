-- Скрипт создания процедуры заполнения витрины формы F101 DM.DM_F101_ROUND_F

create or replace procedure ds.fill_f101_round_f(i_OnDate date)
language plpgsql
as $$
declare
	startDate date;
	endDate date;
	restDate date;
begin
	endDate = i_OnDate - INTERVAL '1 day';
	startDate = i_OnDate - INTERVAL '1 month';
	restDate = startDate - INTERVAL '1 day';
	delete from dm.dm_f101_round_f where from_date = startDate and to_date = endDate;
	insert into dm.dm_f101_round_f
	with actual_accounts as
	(
		select account_rk, left(account_number, 5) as n2, char_type from md_account_d
		where data_actual_date <= endDate and data_actual_end_date >= startDate
	),
	actual_accounts_n2 as 
	(
		select n2, char_type as ap from actual_accounts
		group by n2, char_type
	),
	rub_actual_accounts as
	(
		select account_rk, left(account_number, 5) as n2, char_type as ap from md_account_d
		where data_actual_date <= endDate and data_actual_end_date >= startDate and currency_code in ('810', '643')
	),
	val_actual_accounts as
	(
		select account_rk, left(account_number, 5) as n2, char_type as ap from md_account_d
		where data_actual_date <= endDate and data_actual_end_date >= startDate and currency_code not in ('810', '643')
	),
	rub_start_rest as 
	(
		select n2, ap, sum(balance_out_rub) as rub_start_rest from rub_actual_accounts r join dm.dm_account_balance_f b on r.account_rk = b.account_rk 
		where on_date = restDate
		group by n2, ap
	),
	val_start_rest as 
	(
		select n2, ap, sum(balance_out_rub) as val_start_rest from val_actual_accounts v join dm.dm_account_balance_f b on v.account_rk = b.account_rk 
		where on_date = restDate
		group by n2, ap
	),
	rub_end_rest as 
	(
		select n2, ap, sum(balance_out_rub) as rub_end_rest from rub_actual_accounts r join dm.dm_account_balance_f b on r.account_rk = b.account_rk 
		where on_date = endDate
		group by n2, ap
	),
	val_end_rest as 
	(
		select n2, ap, sum(balance_out_rub) as val_end_rest from val_actual_accounts v join dm.dm_account_balance_f b on v.account_rk = b.account_rk 
		where on_date = endDate
		group by n2, ap
	),
	rub_turn_dt as 
	(
		select n2, ap, sum(debet_amount_rub) as rub_turn_dt from rub_actual_accounts r join dm.dm_account_turnover_f t on r.account_rk = t.account_rk
		where on_date between startDate and endDate
		group by n2, ap
	),
	val_turn_dt as 
	(
		select n2, ap, sum(debet_amount_rub) as val_turn_dt from val_actual_accounts v join dm.dm_account_turnover_f t on v.account_rk = t.account_rk
		where on_date between startDate and endDate
		group by n2, ap
	),
	rub_turn_ct as 
	(
		select n2, ap, sum(credit_amount_rub) as rub_turn_ct from rub_actual_accounts r join dm.dm_account_turnover_f t on r.account_rk = t.account_rk
		where on_date between startDate and endDate
		group by n2, ap
	),
	val_turn_ct as 
	(
		select n2, ap, sum(credit_amount_rub) as val_turn_ct from val_actual_accounts v join dm.dm_account_turnover_f t on v.account_rk = t.account_rk
		where on_date between startDate and endDate
		group by n2, ap
	)
	select
	startDate as FROM_DATE,
	endDate as TO_DATE,
	CHAPTER,
	a.n2 as LEDGER_ACCOUNT,
	a.ap as CHARACTERISTIC,
	coalesce (rub_start_rest, 0) as BALANCE_IN_RUB, 0 as r1,
	coalesce (val_start_rest, 0) as BALANCE_IN_VAL, 0 as r2,
	coalesce (rub_start_rest, 0) + coalesce (val_start_rest, 0) as BALANCE_IN_TOTAL, 0 as r3,
	coalesce (rub_turn_dt, 0) as TURN_DEB_RUB, 0 as r4,
	coalesce (val_turn_dt, 0) as TURN_DEB_VAL, 0 as r5,
	coalesce (rub_turn_dt, 0) + coalesce (val_turn_dt, 0) as TURN_DEB_TOTAL, 0 as r6,
	coalesce (rub_turn_ct, 0) as TURN_CRE_RUB, 0 as r7,
	coalesce (val_turn_ct, 0) as TURN_CRE_VAL, 0 as r8,
	coalesce (rub_turn_ct, 0) + coalesce (val_turn_ct, 0) as TURN_CRE_TOTAL, 0 as r9,
	coalesce (rub_end_rest, 0) as BALANCE_OUT_RUB, 0 as r10,
	coalesce (val_end_rest, 0) as BALANCE_OUT_VAL, 0 as r11,
	coalesce (rub_end_rest, 0) + coalesce (val_end_rest, 0) as BALANCE_OUT_TOTAL, 0 as r12
	from actual_accounts_n2 a 
		left join rub_start_rest on a.n2 = rub_start_rest.n2 and a.ap = rub_start_rest.ap
		left join val_start_rest on a.n2 = val_start_rest.n2 and a.ap = val_start_rest.ap
		left join rub_end_rest on a.n2 = rub_end_rest.n2 and a.ap = rub_end_rest.ap
		left join val_end_rest on a.n2 = val_end_rest.n2 and a.ap = val_end_rest.ap
		left join rub_turn_dt on a.n2 = rub_turn_dt.n2 and a.ap = rub_turn_dt.ap
		left join val_turn_dt on a.n2 = val_turn_dt.n2 and a.ap = val_turn_dt.ap
		left join rub_turn_ct on a.n2 = rub_turn_ct.n2 and a.ap = rub_turn_ct.ap
		left join val_turn_ct on a.n2 = val_turn_ct.n2 and a.ap = val_turn_ct.ap
		inner join md_ledger_account_s led on a.n2 = led.ledger_account::text;
end; $$;

