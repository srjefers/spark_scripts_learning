create table public.tmp_data (
	id bigserial,
	name varchar(50)
);

insert into public.tmp_data (name)
select 'abc'
union all 
select 'def'
union all 
select 'ghi'
union all
select 'jkl';

select * from public.tmp_data;