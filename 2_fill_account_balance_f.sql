truncate table dm.dm_account_balance_f
;
-- ЗАполнение данными за 31.12.2017
insert into dm.dm_account_balance_f (on_date,
								    account_rk,
								    balance_out,
								    balance_out_rub
									)
	select fbf.on_date,
    		fbf.account_rk,
    		fbf.balance_out,
    		fbf.balance_out * coalesce(merd.reduced_cource, 1) as balance_out_rub
	from ds.ft_balance_f fbf 
	left join ds.md_exchange_rate_d merd  on merd.currency_rk = fbf.currency_rk
	where fbf.on_date = '2017-12-31'
		and (merd.data_actual_date <= '2017-12-31' and (
			merd.data_actual_end_date >= '2017-12-31' or merd.data_actual_end_date is null
			)
			)
;
-- Проверка
select * from dm.dm_account_balance_f dabf
;
-- Сама функция

create or replace procedure ds.fill_account_balance_f(i_on_date date)
language plpgsql
as $$
declare
    v_log_id integer; -- идентификатор записи в логе
    v_prev_date date := i_on_date - interval '1 day'; -- предыдущий день
begin
    -- логируем начало работы процедуры
    insert into logs.loading_logs (process_name,
									start_time,
									status)
    values ('fill_account_balance_f',
        	now(),
        	'начало выполнения')
    returning log_id into v_log_id;
-- удаляем старые данные на указанную дату	
delete from dm.dm_account_balance_f 
    where on_date = i_on_date;
-- рассчитываем и вставляем новые остатки
    insert into dm.dm_account_balance_f (on_date,
        									account_rk,
        									balance_out,
        									balance_out_rub
    										)
    	select i_on_date as on_date,
        		mad.account_rk,        
        		case mad.char_type
        	    	when 'А' then coalesce(prev.balance_out, 0) + coalesce(turn.debet_amount, 0) - coalesce(turn.credit_amount, 0)
           			when 'П' then coalesce(prev.balance_out, 0) - coalesce(turn.debet_amount, 0) + coalesce(turn.credit_amount, 0)
        		end as balance_out,
				case mad.char_type
            		when 'А' then coalesce(prev.balance_out_rub, 0) + coalesce(turn.debet_amount_rub, 0) - coalesce(turn.credit_amount_rub, 0)
            		when 'П' then coalesce(prev.balance_out_rub, 0) - coalesce(turn.debet_amount_rub, 0) + coalesce(turn.credit_amount_rub, 0)
        		end as balance_out_rub
		from ds.md_account_d mad
		left join dm.dm_account_balance_f prev on prev.account_rk = mad.account_rk
       										and prev.on_date = v_prev_date -- остаток на предыдущий день
		left join dm.dm_account_turnover_f turn on turn.account_rk = mad.account_rk
       										and turn.on_date = i_on_date -- обороты за текущий день
   		where mad.data_actual_date <= i_on_date
			and (mad.data_actual_end_date > i_on_date
				or mad.data_actual_end_date is null);-- выбираем только актуальные записи на дату;

    update logs.loading_logs
    		set
			end_time = now(),
        	status = 'успешно',
        	records_loaded = (select count(*) from dm.dm_account_balance_f
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
$$;


select * from logs.loading_logs order by 1 desc limit 10
;

select * from dm.dm_account_balance_f --where on_date = '2018-01-12'
;

do $$
declare
    x_date date := date '2018-01-01'; -- начальная дата
begin
    while x_date <= date '2018-01-31' loop
        raise notice 'считаем за дату:%', x_date;
        call ds.fill_account_balance_f(x_date); -- вызов процедуры на текущую дату
        x_date := x_date + interval '1 day';
    end loop;
end $$
;