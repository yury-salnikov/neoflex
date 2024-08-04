--Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня.
with account_balance_w as (
	select account_rk, effective_date, account_in_sum, account_out_sum,
	lead(account_in_sum, 1, NULL) over (partition by account_rk order by effective_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as next_date_in_sum,
	last_value (account_out_sum) over (partition by account_rk order by effective_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_out_sum
	from account_balance t
), account_balance_full as
(
	select account_rk, effective_date, account_in_sum, account_out_sum, coalesce(next_date_in_sum, last_out_sum) as next_date_in_sum from account_balance_w
)
select account_rk, effective_date, account_in_sum, next_date_in_sum as account_out_sum from account_balance_full;