-- group by race and weapon flag
with result as (
	select weapsoc, monrace, count(*) as ct from ussc
	group by monrace, weapsoc
)
select * from result order by ct desc;
