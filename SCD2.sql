create table deaian.trsh_source( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table deaian.trsh_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);


insert into deaian.trsh_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into deaian.trsh_source ( id, val, update_dt ) values ( 2, 'B', now() );
insert into deaian.trsh_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into deaian.trsh_source ( id, val, update_dt ) values ( 4, 'X', now() );
--insert into deaian.trsh_source ( id, val, update_dt ) values ( 5, 'Y', now() );
update deaian.trsh_source set val = 'Z', update_dt = now() where id = 4;
update deaian.trsh_source set val = null, update_dt = now() where id = 4;



------работа с хранилищем (организационная)---


create table deaian.trsh_stg( 
	id integer,
	val varchar(50),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.trsh_stg_del( 
	id integer
);



create table deaian.trsh_target (
	id integer,
	val varchar(50),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1),
	processed_dt timestamp(0)
	
);


------------------------------
--Скрипт инкриментальной загрузки SCD1
--------------------------------
--наш рабочий скрипт. он выполняется всегда целиком вместе
--условно выполняем его раз в час ночи и он должен работать без вмешивания руками


--1. очищаем стейджинг --
--truncate не подходит тк он по дефолту содержит автокомит

delete from deaian.trsh_stg;
delete from deaian.trsh_stg_del;

--2. захват данных с источника-- в processed_dt всегда ложится now()

insert into deaian.trsh_stg ( id, val, update_dt, processed_dt)
select id, val, update_dt, now() from deaian.trsh_source
where update_dt > coalesce ((
   select max_update_dt
   from deaian.trsh_meta
   where schema_name = 'deaian' and table_name = 'trsh_source'
), to_date('1900-01-01','YYYY-MM-DD') ); --если не вставили метаданные

insert into deaian.trsh_stg_del ( id)
select id from deaian.trsh_source;
--при каждой загрузке нужно очищать стейджинг--

--3. Применение данных (накатка) в приемник DDS (вставка)
insert into deaian.trsh_target ( id, val, effective_from, effective_to, deleted_flg, processed_dt )
select 
	stg.id,
	stg.val,
	stg.update_dt, 
	to_date('9999-12-31','YYYY-MM-DD'),
	'N',
	now()
from deaian.trsh_stg stg
left join deaian.trsh_target tgt
on stg.id = tgt.id
where tgt.id is null; --переоткрытия не будет

--таргет приджойнивается по id и в val заносятся данные из джойна

--4. Применение данных в приемник DDS (обновление)
update deaian.trsh_target 
set 
   effective_to = tmp.update_dt - interval '1 second',
   processed_dt = now()
from (
   select 
      stg.id
      , stg.val
      , stg.update_dt

   from deaian.trsh_stg stg
   inner join deaian.trsh_target tgt
   on stg.id = tgt.id
      and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N'
      --в акутальном срезе не должно быть удаленных записей в этой схеме
   where stg.val <> tgt.val or (stg.val is null and tgt.val is not null) 
        or (stg.val is not null and tgt.val is null) --сравнение с null вернет false поэтому улучшаем код
) tmp
where trsh_target.id = tmp.id; --тут типа иннер джойн через where
-- сджойнить стейджинг с таргетом, чтобы в таргете не задваивать данные
--сравнение с null вернет false поэтому улучшаем код


insert into deaian.trsh_target ( id, val, effective_from, effective_to, deleted_flg, processed_dt )
select 
	stg.id,
	stg.val,
	stg.update_dt, 
	to_date('9999-12-31','YYYY-MM-DD'),
	'N',
	now()
from deaian.trsh_stg stg
inner join deaian.trsh_target tgt
on stg.id = tgt.id
      and tgt.effective_to = stg.update_dt - interval '1 second'
      and tgt.deleted_flg = 'N'
      --в акутальном срезе не должно быть удаленных записей в этой схеме
where stg.val <> tgt.val or (stg.val is null and tgt.val is not null) 
        or (stg.val is not null and tgt.val is null)

--5. Применение данных в приемник DDS (удаление)
        
insert into deaian.trsh_target ( id, val, effective_from, effective_to, deleted_flg, processed_dt )
select 
	id,
	val,
	now(), 
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y',
	now()
from deaian.trsh_target 
where id in (
  select 
       tgt.id
  from deaian.trsh_target tgt
  left join deaian.trsh_stg_del stg
  on tgt.id = stg.id
     and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     and tgt.deleted_flg = 'N'
  where stg.id is null
  )
  and effective_to = to_date('9999-12-31','YYYY-MM-DD')
  and deleted_flg = 'N'


update deaian.trsh_target
set 
   effective_to = now() - interval '1 second' --обработать эти now
where id in (
  select 
       tgt.id
  from deaian.trsh_target tgt
  left join deaian.trsh_stg_del stg
  on tgt.id = stg.id
     and tgt.effective_to = to_date('9999-12-31','YYYY-MM-DD')
     and tgt.deleted_flg = 'N'
  where stg.id is null
)
and effective_to = to_date('9999-12-31','YYYY-MM-DD')
and deleted_flg = 'N'
;

--6. Сохраняем состояние загрузки в метаданные 
--если в метаданных нет записи, то вставаляем ее. Если есть, то просто update
insert into deaian.trsh_meta( schema_name, table_name, max_update_dt )
select
	'deaian'
    ,'trsh_source'
    , to_date('1900-01-01','YYYY-MM-DD') 
from deaian.trsh_meta
where not exists (select * from deaian.trsh_meta
                  where schema_name = 'deaian' 
                  and table_name = 'trsh_source');

update deaian.trsh_meta
set max_update_dt = (select max(update_dt)) 
from deaian.trsh_stg)
where schema_name = 'deaian' and table_name = 'trsh_source';

--7. Фиксация транзакции

commit;
