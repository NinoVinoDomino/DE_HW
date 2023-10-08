---ДЗ6

--создаем копию "источника" (staging)



create table if not exists  deaian.trsh_histgroup as 
		(select person, 
		        first_value(class) over (partition by person order by dt), 
		        salary, 
		        class,
                dt effective_from,
       coalesce(cast(lead(dt) over (partition by person  order by dt) - interval '1 day' as date), to_date('9999-12-31' ,'YYYY-MM-DD')) effective_to
from de.histgroup
)


--создаем детализированный слой (хранилище)


create table DEAIAN.trsh_SALARY_HIST as (
     select  person
            , class
            , salary
            , effective_from
            , coalesce(
                cast(
                  lead(effective_from) 
                    over (partition by person order by effective_from)- interval '1 day'
                       as date), to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from (
       select person,
           salary,
           class,
           effective_from,
           effective_to 
       from deaian.trsh_histgroup
       order by person, effective_from
) t

order by person, effective_from
)


--беру в работу таблицы DEAIAN.trsh_SALARY_HIST и DE.SALARY_PAYMENTS


create table if not exists DEAIAN.trsh_SALARY_LOG as (
    
 select 
       dt as PAYMENT_DT,
       sp.person,
       payment,
       sum(payment) over (
             partition by sp.person,  
             extract(year from dt), extract(month from dt) 
             order by dt) as month_paid,
       salary - sum(payment) over (
             partition by sp.person,  
             extract(year from dt), extract(month from dt) 
             order by dt)  as month_rest
 from de.salary_payments sp
 left join deaian.trsh_histgroup th 
 on sp.person = th.person 
 and dt between effective_from and effective_to --условие среза на дату
 order by sp.person, dt
)

