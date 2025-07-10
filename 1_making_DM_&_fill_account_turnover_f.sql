create schema dm;
create table if not exists dm.dm_account_turnover_f (
										on_date date
										,account_rk numeric
										,credit_amount numeric(23,8)
										,credit_amount_rub numeric(23,8)
										,debet_amount numeric(23,8)
										,debet_amount_rub numeric(23,8)
										)
;
create table if not exists dm.dm_account_balance_f (
										on_date date 
										,account_rk numeric
										,balance_out numeric(23,8)
										,balance_out_rub numeric(23,8)
										)
;
create table if not exists dm.dm_f101_round_f(from_date date
										,to_date date
										,chapter varchar(1)
										,ledger_account varchar(5)
										,characteristic varchar(1)
										,balance_in_rub numeric(23,8)
										,balance_in_val numeric(23,8)
										,balance_in_total numeric(23,8)
										,turn_deb_rub numeric(23,8)
										,turn_deb_val numeric(23,8)
										,turn_deb_total numeric(23,8)
										,turn_cre_rub numeric(23,8)
										,turn_cre_val numeric(23,8)
										,turn_cre_total numeric(23,8)
										,balance_out_rub numeric(23,8)
										,balance_out_val numeric(23,8)
										,balance_out_total numeric(23,8)
										)
;



create or replace procedure ds.fill_account_turnover_f(i_on_date date)
language plpgsql
as $$
declare
    v_log_id integer;
begin
    insert into logs.loading_logs (
        process_name, start_time, status
    ) values (
        'fill_account_turnover_f',
        now(),
        'начало выполнения'
    )
    returning log_id into v_log_id;
    delete from dm.dm_account_turnover_f -- Удаляем старые записи за эту дату
    where on_date = i_on_date;
	insert into dm.dm_account_turnover_f (
        								on_date
										,account_rk
										,credit_amount
										,credit_amount_rub	
										,debet_amount
										,debet_amount_rub
    									)
    select oper_date as on_date,
    			account_rk,
    			sum(credit_amount) as credit_amount,
  				sum(credit_amount_rub) as credit_amount_rub,
    			sum(debet_amount) as debet_amount,
    			sum(debet_amount_rub) as debet_amount_rub
--  в подзапросе создадим две таблицы, потом объединим их union all, после чего сгруппируем по account_rk
from (	select fpf.oper_date,
        	fpf.credit_account_rk as account_rk,
        	fpf.credit_amount,
        	fpf.credit_amount * coalesce(merd.reduced_cource, 1) as credit_amount_rub, --coalesce заменит множитель на 1, если его нет
        	0 as debet_amount, --пока вставим нули
        	0 as debet_amount_rub
    	from ds.ft_posting_f fpf
    	left join ds.md_account_d mad on mad.account_rk = fpf.credit_account_rk 
    	left join ds.md_exchange_rate_d merd on merd.currency_rk = mad.currency_rk 
    							and fpf.oper_date>=merd.data_actual_date and fpf.oper_date <= merd.data_actual_end_date
    where fpf.oper_date = i_on_date
    union all
       select fpf.oper_date,
        		fpf.debet_account_rk as account_rk,
        		0 as credit_amount, --анлогично предыдущему, но здесь 0 в кредите
        		0 as credit_amount_rub,
        		fpf.debet_amount,
        		fpf.debet_amount * coalesce(merd.reduced_cource, 1) as debet_amount_rub
    	from ds.ft_posting_f fpf
    	left join ds.md_account_d mad on mad.account_rk = fpf.debet_account_rk 
    	left join ds.md_exchange_rate_d merd on merd.currency_rk = mad.currency_rk 
    							and fpf.oper_date>=merd.data_actual_date and fpf.oper_date <= merd.data_actual_end_date
    where fpf.oper_date = i_on_date
)
group by oper_date, account_rk
order by account_rk;
    --обновляем лог если успех
    update logs.loading_logs
    set 
        end_time = now(),
        status = 'успешно',
        records_loaded = (
            select count(*) from dm.dm_account_turnover_f
            where on_date = i_on_date
        )
    where log_id = v_log_id;
exception
when others then
begin
	if v_log_id is not null then
		update logs.loading_logs
                set 
                end_time = now(),
                status = '!ОШИБКА',
                error_message = sqlerrm
                where log_id = v_log_id;
            	end if;
        exception
            when others then -- почему то блок обработки ошибок не давал выполняться процедуре, пришоось ввести проверку v_log_id
                null;		 -- и вторую exception
        end;
end;
$$
;

truncate table dm.dm_account_turnover_f
;
select * from dm.dm_account_turnover_f;


do $$
declare
    x_date date := date '2018-01-01'; -- начальная дата
begin
    while x_date <= date '2018-01-31' loop
        raise notice 'считаем за дату:%', x_date;
        call ds.fill_account_turnover_f(x_date); -- вызов процедуры на текущую дату
        x_date := x_date + interval '1 day';
    end loop;
end $$
;

select * from dm.dm_account_turnover_f datf order by 1
;

select * from logs.loading_logs ll 
order by 1 desc 
limit 32
 ;