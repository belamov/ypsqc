--------------------------------------------
------------------ Этап 1 ------------------
--------------------------------------------
drop database if exists sprint_2;

create database sprint_2;

-- /usr/bin/pg_restore --dbname=sprint_2 --username=postgres --host=localhost --port=5433 /home/belamov/Downloads/sprint2_dump.sql
-- или восстановить бд из дампа в sprint_2 любым другим способом

drop table if exists cafe.restaurants cascade;
drop type if exists cafe.restaurant_type cascade;
drop table if exists cafe.managers cascade;
drop table if exists cafe.sales cascade;
drop table if exists cafe.restaurant_manager_work_dates cascade;

create type cafe.restaurant_type as enum ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

create table cafe.restaurants
(
    restaurant_uuid uuid default gen_random_uuid() not null
        constraint restaurants_pk
            primary key,
    name            text                           not null,
    position        public.geography(point)        not null,
    type            cafe.restaurant_type           not null,
    menu            jsonb
);

insert into cafe.restaurants (name, position, type, menu)
select cafe_name, public.st_makepoint(longitude, latitude), type::cafe.restaurant_type, menu
from raw_data.sales
join raw_data.menu using (cafe_name)
group by cafe_name, longitude, latitude, type, menu;

create table cafe.managers
(
    manager_uuid uuid default gen_random_uuid() not null
        constraint managers_pk
            primary key,
    name            text                           not null,
    phone           text
);

insert into cafe.managers (name, phone)
select manager, manager_phone
from raw_data.sales
group by manager, manager_phone;

create table cafe.restaurant_manager_work_dates
(
    restaurant_uuid uuid      not null
        constraint restaurant_manager_work_dates_restaurants_restaurant_uuid_fk
            references cafe.restaurants,
    manager_uuid    uuid      not null
        constraint restaurant_manager_work_dates_managers_manager_uuid_fk
            references cafe.managers,
    period          daterange not null,
    constraint restaurant_manager_work_dates_pk
        primary key (restaurant_uuid, manager_uuid)
);

insert into cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, period)
select r.restaurant_uuid, m.manager_uuid, daterange(min(s.report_date), max(s.report_date), '[]')
from raw_data.sales s
join cafe.restaurants r on s.cafe_name=r.name
join cafe.managers m on s.manager=m.name
group by r.restaurant_uuid, m.manager_uuid;

create table cafe.sales
(
    date            date not null,
    restaurant_uuid uuid not null
        constraint sales_restaurants_restaurant_uuid_fk
            references cafe.restaurants,
    avg_check       numeric,
    constraint sales_pk
        primary key (date, restaurant_uuid)
);

insert into cafe.sales (date, restaurant_uuid, avg_check)
select s.report_date, r.restaurant_uuid, s.avg_check
from raw_data.sales s
join cafe.restaurants r on s.cafe_name=r.name;

--------------------------------------------
------------------ Этап 2 ------------------
--------------------------------------------

-- Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый
-- высокий средний чек. Создайте представление, которое покажет топ-3 заведений
-- внутри каждого типа заведения по среднему чеку за все даты.
-- Столбец со средним чеком округлите до второго знака после запятой.
create view v_top_avg_by_type as
with
    top as (
        select
            r.name,
            r.type,
            row_number() over (partition by r.type order by avg(avg_check) desc) as rank,
            round(avg(s.avg_check),2) as avg
        from cafe.sales s
        join cafe.restaurants r using (restaurant_uuid)
        group by 1,2
    )
select top.name, top.type, top.avg
from top
where top.rank<=3;

-- Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый высокий средний чек.
-- Создайте представление, которое покажет топ-3 заведений внутри каждого
-- типа заведения по среднему чеку за все даты.
-- Столбец со средним чеком округлите до второго знака после запятой.
create materialized view v_year_by_year_avg as
with
    avg_by_year as (
        select s.restaurant_uuid, extract('year' from s.date) as year, round(avg(avg_check), 2) as avg
        from cafe.sales s
        group by s.restaurant_uuid, extract('year' from s.date)
    )
select
    a.year as "Год",
    r.name as "Название заведения",
    r.type as "Тип заведения",
    a.avg as "Средний чек в этом году",
    lag(a.avg) over (partition by a.restaurant_uuid order by a.year asc) as "Средний чек в предыдущем году",
    round(((a.avg / lag(a.avg) over (partition by a.restaurant_uuid order by a.year asc)) - 1) * 100,2) AS "Изменение среднего чека в %"
from avg_by_year a
join cafe.restaurants r using (restaurant_uuid)
where a.year!=2023
order by a.restaurant_uuid, a.year;

-- Найдите топ-3 заведения, где чаще всего менялся менеджер за весь период.
select r.name, count(distinct manager_uuid) as count
from cafe.restaurant_manager_work_dates d
join cafe.restaurants r using (restaurant_uuid)
group by r.name
order by count desc
limit 3;

-- Найдите пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведите все.
with rest_pizza as (
    select r.name, jsonb_each_text(menu -> 'Пицца') as pizza
    from cafe.restaurants r
),
rest_rank as (
    select name, count(pizza) as pizza_count, dense_rank() over (order by count(pizza) desc) as rank
from rest_pizza
group by name
)
select name, pizza_count from rest_rank where rank=1;

-- Найдите самую дорогую пиццу для каждой пиццерии.
with
    rest_pizza as (
        select r.name, key as pizza_name, value::int as pizza_price
        from cafe.restaurants r, jsonb_each(r.menu->'Пицца')
        where r.menu->'Пицца' is not null
    ),
    rest_pizza_rank as (
        select *, row_number() over (partition by name order by pizza_price desc) as rank
        from rest_pizza
    )
select name, pizza_name, pizza_price
from rest_pizza_rank
where rank=1;

-- Найдите два самых близких друг к другу заведения одного типа.
with
    rest_dist as (
        select r1.name as name_1, r2.name as name_2, r1.type, public.st_distance(r1.position, r2.position) as dist
        from cafe.restaurants r1
        join cafe.restaurants r2 on r1.type=r2.type and r1.restaurant_uuid!=r2.restaurant_uuid
    )
select name_1, name_2, type, min(dist) as dist
from rest_dist
group by type, name_1, name_2, dist
order by dist asc
limit 1;

-- Найдите район с самым большим количеством заведений и район с самым маленьким количеством заведений.
-- Первой строчкой выведите район с самым большим количеством заведений, второй — с самым маленьким.
with
    dist_count as (
        select d.district_name, count(*) as rest_count
        from cafe.restaurants r
        join cafe.districts d on public.st_within(r.position::public.geometry, d.district_geom)
        group by d.district_name
    )
select *
from dist_count
where rest_count=(select max(rest_count) from dist_count)
union
select *
from dist_count
where rest_count=(select min(rest_count) from dist_count)
order by rest_count desc;