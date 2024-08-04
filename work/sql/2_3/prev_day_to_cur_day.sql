--Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, то корректным выбирается значение account_out_sum предыдущего дня.
with account_balance_w as (
select account_rk, effective_date, account_in_sum, account_out_sum,
lag(account_out_sum, 1, NULL) over (partition by account_rk order by effective_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as prev_date_out_sum,
first_value (account_in_sum) over (partition by account_rk order by effective_date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_in_sum
from account_balance t
), account_balance_full as (
select account_rk, effective_date, account_in_sum, account_out_sum, coalesce(prev_date_out_sum, first_in_sum) as prev_date_out_sum from account_balance_w
)
select account_rk, effective_date, prev_date_out_sum as account_in_sum, account_out_sum from account_balance_full;