drop schema car_shop cascade;
drop schema raw_data cascade;

CREATE SCHEMA raw_data;

create table raw_data.sales
(
    id                   integer,
    auto                 text,
    gasoline_consumption real,
    price                double precision,
    date                 text,
    person_name          text,
    phone                text,
    discount             integer,
    brand_origin         text
);

COPY raw_data.sales FROM '/tmp/cars.csv' WITH CSV HEADER NULL 'null';

CREATE SCHEMA car_shop;

CREATE TABLE car_shop.countries (
    id serial primary key,
    name character varying unique
);

CREATE TABLE car_shop.colors (
    id serial primary key,
    name character varying unique
);

CREATE TABLE car_shop.car_brands (
    id serial primary key,
    name character varying unique NULLS DISTINCT ,
    country_id integer references car_shop.countries (id)
        match simple on update no action on delete no action
);


create table car_shop.car_models (
  id serial primary key,
  name character varying not null,
  gasoline_consumption numeric,
  brand_id integer not null,
  foreign key (brand_id) references car_shop.car_brands (id)
  match simple on update no action on delete no action
);
create unique index car_models_unique on car_shop.car_models using btree (name, brand_id);

create table car_shop.clients (
  id serial primary key,
  name character varying,
  phone character varying
);
create unique index clients_unique_name_phone on car_shop.clients using btree (name, phone);

create table car_shop.sales (
  id serial primary key,
  car_model_id integer,
  color_id integer,
  date date,
  client_id integer,
  discount smallint not null default 0,
  price numeric,
  foreign key (car_model_id) references car_shop.car_models (id)
  match simple on update no action on delete no action,
  foreign key (client_id) references car_shop.clients (id)
  match simple on update no action on delete no action,
  foreign key (color_id) references car_shop.colors (id)
  match simple on update no action on delete no action
);

--
--
--
-- ЗАПОЛНЕНИЕ ТАБЛИЦ
--
--

insert into car_shop.colors (name)
select
    lower(trim(split_part(auto, ',', -1)))
from raw_data.sales
on conflict do nothing;

insert into car_shop.countries (name)
select
    brand_origin
from raw_data.sales
where brand_origin is not null
on conflict do nothing;

insert into car_shop.car_brands (name, country_id)
select
    split_part(auto, ' ', 1),
    c.id
from raw_data.sales
left join car_shop.countries c on brand_origin = c.name
on conflict do nothing;

insert into car_shop.car_models (name, gasoline_consumption, brand_id)
select
    trim(substr(split_part(auto, ',', 1), strpos(split_part(auto, ',', 1), ' '))),
    gasoline_consumption,
    cb.id
from raw_data.sales
left join car_shop.car_brands cb on cb.name=split_part(auto, ' ', 1)
on conflict do nothing;

insert into car_shop.clients (name, phone)
select person_name, phone
from raw_data.sales
on conflict do nothing;

insert into car_shop.sales (id, car_model_id, date, client_id, price, discount, color_id)
select rd.id, cm.id, rd.date::date, c.id, rd.price, rd.discount, col.id
from raw_data.sales rd
left join car_shop.car_models cm on cm.name=trim(substr(split_part(auto, ',', 1), strpos(split_part(auto, ',', 1), ' ')))
left join car_shop.clients c on c.name = rd.person_name and c.phone=rd.phone
left join car_shop.colors col on col.name=trim(split_part(auto, ',', -1));

--
--
--ЗАДАНИЯ
--
--

-- Напишите запрос, который выведет процент моделей машин, у которых нет параметра gasoline_consumption.
select 100.0 * (count(*) - count(gasoline_consumption)) / count(*) as nulls_percentage_gasoline_consumption
from car_shop.car_models;

-- Напишите запрос, который покажет название бренда и среднюю цену его автомобилей
-- в разбивке по всем годам с учётом скидки.
-- Итоговый результат отсортируйте по названию бренда и году в восходящем порядке.
-- Среднюю цену округлите до второго знака после запятой.
select cb.name as brand_name, extract(year from s.date) as year, round(avg(price), 2) as price_avg
from car_shop.sales s
join car_shop.car_models cm on s.car_model_id = cm.id
join car_shop.car_brands cb on cm.brand_id = cb.id
group by cb.name, extract(year from s.date)
order by cb.name, extract(year from s.date);

-- Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.
-- Результат отсортируйте по месяцам в восходящем порядке.
-- Среднюю цену округлите до второго знака после запятой.
select extract(month from date) as month, 2022 as year, round(avg(price), 2) as avg_price
from car_shop.sales
where extract(year from date)=2022
group by extract(month from date);

--Используя функцию STRING_AGG, напишите запрос, который выведет список купленных машин у каждого пользователя через запятую.
-- Пользователь может купить две одинаковые машины — это нормально.
-- Название машины покажите полное, с названием бренда — например: Tesla Model 3.
-- Отсортируйте по имени пользователя в восходящем порядке. Сортировка внутри самой строки с машинами не нужна.
select c.name as person, string_agg(cb.name || ' ' || cm.name, ', ')
from car_shop.sales s
join car_shop.clients c on s.client_id=c.id
join car_shop.car_models cm on s.car_model_id = cm.id
left join car_shop.car_brands cb on cm.brand_id = cb.id
group by c.name
order by c.name;

--Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля
-- с разбивкой по стране без учёта скидки.
-- Цена в колонке price дана с учётом скидки.
select c.name as brand_origin, max(price*100/(100-discount)) as price_max, min(price*100/(100-discount)) as price_min
from car_shop.sales s
join car_shop.car_models cm on s.car_model_id = cm.id
join car_shop.car_brands cb on cm.brand_id = cb.id
left join car_shop.countries c on cb.country_id = c.id
group by c.name;

--Напишите запрос, который покажет количество всех пользователей из США.
-- Это пользователи, у которых номер телефона начинается на +1.
select count(*)
from car_shop.clients
where strpos(phone, '+1') = 1