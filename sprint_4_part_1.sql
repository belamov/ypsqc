create database sprint_4;

--/usr/bin/pg_restore --dbname=sprint_4 --clean --username=postgres --host=localhost --port=5433 /home/belamov/Downloads/project_4.sql

---------------
-- Задание 1 --
---------------

-- Клиенты сервиса начали замечать, что после нажатия на кнопку Оформить заказ система на какое-то время подвисает.
-- Вот команда для вставки данных в таблицу orders, которая хранит общую информацию о заказах:

INSERT INTO orders
(order_id, order_dt, user_id, device_type, city_id, total_cost, discount,
 final_cost)
SELECT MAX(order_id) + 1,
       current_timestamp,
       '329551a1-215d-43e6-baee-322f2467272d',
       'Mobile',
       1,
       1000.00,
       null,
       1000.00
FROM orders;

--Чтобы лучше понять, как ещё используется в запросах таблица orders, выполните запросы:

SELECT order_dt
FROM orders
WHERE order_id = 153;

SELECT order_id
FROM orders
WHERE order_dt > current_date::timestamp;

SELECT count(*)
FROM orders
WHERE user_id = '329551a1-215d-43e6-baee-322f2467272d';

-- Не переживайте, если какой-то запрос ничего не вернул, — это нормально. Пустой результат — тоже результат.
-- Проанализируйте возможные причины медленной вставки новой строки в таблицу orders.

-- Решение:
-- 1. Данные вставляются медленно по двум причинам:
--     - При каждой вставке определяется максимальное значение order_id.
--       Вместо такого ручного определения следующего айдишника стоит использовать автоинкримент:
create sequence order_id_sec owned by public.orders.order_id;
select setval('order_id_sec', (select max(order_id) from orders));
alter table orders
    alter column order_id set default nextval('order_id_sec');
--     - Для таблицы заказов создано много индексов, которые не используются
--       в запросах (а при изменении записей в таблице все затрагиваемые индексы перестраиваются, что занимает ресурсы):
--       можем найти неиспользуемые индексы запросом
select indexrelname
from pg_stat_user_indexes
where idx_scan = 0
  and relname = 'orders';
--       и удалить их

---------------
-- Задание 2 --
---------------

-- Клиенты сервиса в свой день рождения получают скидку.
-- Расчёт скидки и отправка клиентам промокодов происходит на стороне сервера приложения.
-- Список клиентов возвращается из БД в приложение таким запросом:
SELECT user_id::text::uuid,
       first_name::text,
       last_name::text,
       city_id::bigint,
       gender::text
FROM users
WHERE city_id::integer = 4
  AND date_part('day', to_date(birth_date::text, 'yyyy-mm-dd'))
    = date_part('day', to_date('31-12-2023', 'dd-mm-yyyy'))
  AND date_part('month', to_date(birth_date::text, 'yyyy-mm-dd'))
    = date_part('month', to_date('31-12-2023', 'dd-mm-yyyy'));
-- Каждый раз список именинников формируется и возвращается недостаточно быстро. Оптимизируйте этот процесс.

-- Мы можем упростить фильтрацию, избавившшись от лишних функций, передавая от сервера приложения день и месяц как два параметра:

SELECT user_id::text::uuid,
       first_name::text,
       last_name::text,
       city_id::bigint,
       gender::text
FROM users
WHERE city_id::integer = 4
  AND extract(day from birth_date::date) = 31
  AND extract(month from birth_date::date) = 12;

-- также можно дополнительно преобразовать колонку с датой рождения в тип date, чтобы не конвертировать значение во врея запроса
alter table public.users
    alter column birth_date type date using birth_date::date;

---------------
-- Задание 3 --
---------------

-- Также пользователи жалуются, что оплата при оформлении заказа проходит долго.
-- Разработчик сервера приложения Матвей проанализировал ситуацию и заключил, что оплата «висит» из-за
-- того, что выполнение процедуры add_payment требует довольно много времени по меркам БД.
-- Найдите в базе данных эту процедуру и подумайте, как можно ускорить её работу.

-- Процедура add_payment заведена так:
create procedure add_payment(IN p_order_id bigint, IN p_sum_payment numeric)
    language plpgsql
as
$$
BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());

    INSERT INTO payments (payment_id, order_id, payment_sum)
    VALUES (nextval('payments_payment_id_sq'), p_order_id, p_sum_payment);

    INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    SELECT NEXTVAL('sales_sale_id_sq'), statement_timestamp(), user_id, p_sum_payment
    FROM orders
    WHERE order_id = p_order_id;
END;
$$;

-- тут можно объединить таблицы payments и sales - непонятно, зачем нужно такое разделение
-- вставка в одну таблицу будет выполняться быстрее
create table public.sales_new
(
    sale_id  serial primary key,
    order_id bigint,
    sale_dt  timestamp with time zone,
    user_id  uuid,
    amount   numeric(14, 2)
);
drop procedure add_payment(p_order_id bigint, p_sum_payment numeric);
create procedure add_payment(IN p_order_id bigint, IN p_sum_payment numeric)
    language plpgsql
as
$$
BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());

    INSERT INTO sales_new(sale_dt, order_id, user_id, amount)
    SELECT statement_timestamp(), p_order_id, user_id, p_sum_payment
    FROM orders
    WHERE order_id = p_order_id;
END;
$$;

---------------
-- Задание 4 --
---------------

-- Все действия пользователей в системе логируются и записываются в таблицу user_logs.
-- Потом эти данные используются для анализа — как правило, анализируются данные за текущий квартал.
-- Время записи данных в эту таблицу сильно увеличилось, а это тормозит практически все действия пользователя.
-- Подумайте, как можно ускорить запись.
-- Вы можете сдать решение этой задачи без скрипта или — попробовать написать скрипт. Дерзайте!

-- Мы можем партицировать таблицу логов - по кварталам

---------------
-- Задание 5 --
---------------

-- Маркетологи сервиса регулярно анализируют предпочтения различных возрастных групп. Для этого они формируют отчёт:
-- day	age	     spicy	  fish	  meat
--      0–20
--     20–30
--     30–40
--     40–100
-- В столбцах spicy, fish и meat отображается, какой % блюд, заказанных каждой категорией пользователей, содержал эти признаки.
-- В возрастных интервалах верхний предел входит в интервал, а нижний — нет.
-- Также по правилам построения отчётов в них не включается текущий день.
-- Администратор БД Серёжа заметил, что регулярные похожие запросы от разных маркетологов нагружают базу, и в результате увеличивается время работы приложения.
-- Подумайте с точки зрения производительности, как можно оптимально собирать и хранить данные для такого отчёта.
-- В ответе на это задание не пишите причину — просто опишите ваш способ получения отчёта и добавьте соответствующий скрипт.

-- Можем добавить материализованное представление, чтобы не выполнять одни и те же запросы
-- Его можно будет формировать в конце дня, поскольку текущий день в отчет не включается, а предыдущие дни уже не меняются
create materialized view report as
(
with user_age_group as (select trim(user_id)::uuid as user_id,
                               case
                                   when extract(year from age(birth_date::date)) <= 20 then '0-20'
                                   when extract(year from age(birth_date::date)) between 21 and 30 then '20-30'
                                   when extract(year from age(birth_date::date)) between 31 and 40 then '30-40'
                                   else '40-100'
                                   end             as age_group
                        from users)
select o.order_dt::date                      as day,
       uag.age_group                         as age,
       sum(count * spicy) / sum(count) * 100 as spicy,
       sum(count * fish) / sum(count) * 100  as fixh,
       sum(count * meat) / sum(count) * 100  as meat
from orders o
         join order_items oi on o.order_id = oi.order_id
         join dishes d on d.object_id = oi.item
         join user_age_group uag on uag.user_id = o.user_id
group by o.order_dt::date, uag.age_group
    )

