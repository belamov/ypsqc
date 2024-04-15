create database sprint_4_part_2;

--/usr/bin/pg_restore --dbname=sprint_4_part_2 --clean --username=postgres --host=localhost --port=5433 /home/belamov/Downloads/project_4_part2.sql
SHOW config_file;
CREATE EXTENSION pg_stat_statements;

-- Ниже — пользовательские скрипты, которые выполняются на базе данных.
-- Выполните их на своём компьютере.
-- Проверьте, что в вашей СУБД включён модуль pg_stat_statements — это обязательное условие.
-- Вспомнить, как подключить модуль можно в третьем уроке третьей темы.

SELECT pg_stat_statements_reset();

-- 1
-- вычисляет среднюю стоимость блюда в определенном ресторане
SELECT avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON dp.dishes_id = d.object_id
WHERE d.rest_id LIKE '%14ce5c408d2142f6bd5b7afad906bc7e%'
	AND dp.date_begin::date <= current_date
	AND (dp.date_end::date >= current_date
		OR dp.date_end IS NULL);

-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );

-- 3
-- выводит id и имена пользователей, фамилии которых входят в список
SELECT u.user_id, u.first_name
FROM users u
WHERE u.last_name IN ('КЕДРИНА', 'АДОА', 'АКСЕНОВА', 'АЙМАРДАНОВА', 'БОРЗЕНКОВА', 'ГРИПЕНКО', 'ГУЦА'
                     , 'ЯВОРЧУКА', 'ХВИЛИНА', 'ШЕЙНОГА', 'ХАМЧИЧЕВА', 'БУХТУЕВА', 'МАЛАХОВЦЕВА', 'КРИСС'
                     , 'АЧАСОВА', 'ИЛЛАРИОНОВА', 'ЖЕЛЯБИНА', 'СВЕТОЗАРОВА', 'ИНЖИНОВА', 'СЕРДЮКОВА', 'ДАНСКИХА')
ORDER BY 1 DESC;

-- 4
-- ищет все салаты в списке блюд
SELECT d.object_id, d.name
FROM dishes d
WHERE d.name LIKE 'salat%';

-- 5
-- определяет максимальную и минимальную сумму заказа по городу
SELECT max(p.payment_sum) max_payment, min(p.payment_sum) min_payment
FROM payments p
    JOIN orders o ON o.order_id = p.order_id
WHERE o.city_id = 2;

-- 6
-- ищет всех партнеров определенного типа в определенном городе
SELECT p.id partner_id, p.chain partner_name
FROM partners p
    JOIN cities c ON c.city_id = p.city_id
WHERE p.type = 'Пекарня'
	AND c.city_name = 'Владивосток';

-- 7
-- ищет действия и время действия определенного посетителя
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

-- 8
-- ищет логи за текущий день
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

-- 9
-- определяет количество неоплаченных заказов
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

-- 10
-- определяет долю блюд дороже 1000
SELECT (SELECT count(*)
	    FROM dishes_prices dp
	    WHERE dp.date_end IS NULL
		    AND dp.price > 1000.00)::NUMERIC / count(*)::NUMERIC
FROM dishes_prices
WHERE date_end IS NULL;

-- 11
-- отбирает пользователей определенного города, чей день рождения находится в интервале +- 3 дня от текущей даты
SELECT user_id, current_date - birth_date
FROM users
WHERE city_id = 1
	AND birth_date >= current_date - 3
	AND birth_date <= current_date + 3;

-- 12
-- вычисляет среднюю стоимость блюд разных категорий
SELECT 'average price with fish', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.fish = 1
UNION
SELECT 'average price with meat', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.meat = 1
UNION
SELECT 'average price of spicy food', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.spicy = 1
ORDER BY 2;

-- 13
-- ранжирует города по общим продажам за определенный период
SELECT ROW_NUMBER() OVER( ORDER BY sum(o.final_cost) DESC),
	c.city_name,
	sum(o.final_cost)
FROM cities c
    JOIN orders o ON o.city_id = c.city_id
WHERE order_dt >= to_timestamp('01.01.2021 00-00-00', 'dd.mm.yyyy hh24-mi-ss')
	AND order_dt < to_timestamp('02.01.2021', 'dd.mm.yyyy hh24-mi-ss')
GROUP BY c.city_name;

-- 14
-- вычисляет количество заказов определенного пользователя
SELECT COUNT(*)
FROM orders
WHERE user_id = '0fd37c93-5931-4754-a33b-464890c22689';

-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
	SELECT item
	FROM (SELECT item, SUM(count) AS total_sales
		  FROM order_items oi
		  GROUP BY 1) dishes_sales
	WHERE dishes_sales.total_sales > (
		SELECT SUM(t.total_sales)/ COUNT(*)
		FROM (SELECT item, SUM(count) AS total_sales
			FROM order_items oi
			GROUP BY
				1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;

-- Ваша задача — найти пять самых медленных скриптов и оптимизировать их.
-- Важно: при оптимизации в этой части проекта нельзя менять структуру БД.
-- В решении укажите способ, которым вы искали медленные запросы, а также для каждого из пяти запросов:
--   - Составьте план запроса до оптимизации.
--   - Укажите общее время выполнения скрипта до оптимизации (вы можете взять его из параметра actual time в плане запроса).
--   - Отметьте узлы с высокой стоимостью и опишите, как их можно оптимизировать.
--   - Напишите и вложите в решение все необходимые скрипты для оптимизации запроса.
--   - Составьте план оптимизированного запроса.
--   - Опишите, что изменилось в плане запроса после оптимизации.
--   - Укажите общее время выполнения запроса после оптимизации.
-- План запроса вы составляете для себя.
-- Опираясь на план, в решении опишите словами проблемные места и что стало лучше после изменений.
-- Можно частично скопировать план, чтобы показать самые важные места.
-- Не прикладывайте скриншоты плана запроса к решению — в таком формате ревьюер не сможет их прочитать.
-- В двух самых тяжёлых запросах можно сократить максимальную стоимость в несколько тысяч раз.
-- В двух менее тяжёлых запросах можно увеличить производительность примерно в 100 и в 6 раз.
-- В оставшемся запросе достаточно повысить производительность на 30%.

-- найде айди нашей бд
 SELECT oid, datname FROM pg_database;
-- айди наей бд - 32122, запомни его


--найдем 5 саых долгих запросов
SELECT
    query,
    ROUND(mean_exec_time::numeric,2),
    ROUND(total_exec_time::numeric,2),
    ROUND(min_exec_time::numeric,2),
    ROUND(max_exec_time::numeric,2),
    calls,
    rows
FROM pg_stat_statements
WHERE dbid = 32122 ORDER BY mean_exec_time DESC
LIMIT 5;

-- top 5:
-- "SELECT count(*)
-- FROM order_statuses os
--     JOIN orders o ON o.order_id = os.order_id
-- WHERE (SELECT count(*)
-- 	   FROM order_statuses os1
-- 	   WHERE os1.order_id = o.order_id AND os1.status_id = $1) = $2
-- 	AND o.city_id = $3"
--
-- "SELECT *
-- FROM user_logs
-- WHERE datetime::date > current_date"
--
-- "SELECT event, datetime
-- FROM user_logs
-- WHERE visitor_uuid = $1
-- ORDER BY 2"
--
-- "SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
-- FROM order_statuses os
--     JOIN orders o ON o.order_id = os.order_id
--     JOIN statuses s ON s.status_id = os.status_id
-- WHERE o.user_id = $1::uuid
-- 	AND os.status_dt IN (
-- 	SELECT max(status_dt)
-- 	FROM order_statuses
-- 	WHERE order_id = o.order_id
--     )"
--
-- "SELECT d.name, SUM(count) AS orders_quantity
-- FROM order_items oi
--     JOIN dishes d ON d.object_id = oi.item
-- WHERE oi.item IN (
-- 	SELECT item
-- 	FROM (SELECT item, SUM(count) AS total_sales
-- 		  FROM order_items oi
-- 		  GROUP BY 1) dishes_sales
-- 	WHERE dishes_sales.total_sales > (
-- 		SELECT SUM(t.total_sales)/ COUNT(*)
-- 		FROM (SELECT item, SUM(count) AS total_sales
-- 			FROM order_items oi
-- 			GROUP BY
-- 				1) t)
-- )
-- GROUP BY 1
-- ORDER BY orders_quantity DESC"

--------------------------------------------
-- топ 1 по медленности выполнения - запрос:
--------------------------------------------
-- 9
-- определяет количество неоплаченных заказов
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

-- произведем анализ этого запроса:
-- фактическое время выполнения до оптимизации: 23103.857
-- неясно, зачем вообще нам соединять таблицу заказов с историей статусов заказов, если запрос возвращает только количество неоплаченных заказов
-- поэтому мы можем убрать этот джойн
-- также можно заметить, что мы используем подзапрос в фильтрации, причем нам интересно только наличие или отсутсвие
-- заказа с определенным статусом в истории изенения статуса, значит операция по подсчету (узел aggregate) не имеет смысла
-- можем заменить его оператором exists
-- в итоге у нас получится такой запрос:
SELECT count(*)
FROM orders o
WHERE not exists(SELECT true
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2)
	AND o.city_id = 1;
-- этот запрос планировщик выполняет методом hash join, что намного быстрее nested loop
-- его фактическое время выполнения - 22.081, что в 1046 раз быстрее оригинального запроса
-- но в нем все еще происходит полное сканирование таблицы истории статусов, что занимает больше всего времени
-- можем добавить индекс на поле status_id, чтобы не сканировать всю таблицу при поиске нужного статуса:
create index idx_order_statuses_status_id on order_statuses(status_id);
-- теперь строки выбираются с помощью bitmap index scan
-- фактическое время оптимизированного запроса - 8.34, что в 2770 раз быстрее оригинального запроса

--------------------------------------------
-- топ 2 по медленности выполнения - запрос:
--------------------------------------------
-- 8
-- ищет логи за текущий день
SELECT *
FROM user_logs
WHERE datetime::date > current_date;
-- врея выполнения запроса до оптиизации: 688.039
-- анализ этого запроса показывает, что таблица партицирована, и планировщик полностью сканирует все партиции в таблице
-- это происходит потому, что мы кастуем поле в дату, а индекс построен по полу таймстепа - и индекс не используется
-- чтобы задействовать индекс, нужно убрать преобразование типа
SELECT *
FROM user_logs
WHERE datetime >= current_date;
-- скорость данного запроса - 0.017, что в 40к быстрее, чем оригинальный запрос


--------------------------------------------
-- топ 3 по медленности выполнения - запрос:
--------------------------------------------
-- 7
-- ищет действия и время действия определенного посетителя
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;
-- время выполнения запроса до оптиизации: 92119.9
-- анализ запроса показал, что планировщик полностью сканирует таблицы логов
-- чтобы избавиться от full scan, мы можем добавить покрывающий индекс, включив в него требуемые в запросе поля:
create index idx_user_logs_visitor_id on user_logs(visitor_uuid) include (event, datetime);
create index idx_user_logs_y2021q2_visitor_id on user_logs_y2021q2(visitor_uuid) include (event, datetime);
create index idx_user_logs_y2021q3_visitor_id on user_logs_y2021q3(visitor_uuid) include (event, datetime);
create index idx_user_logs_y2021q4_visitor_id on user_logs_y2021q4(visitor_uuid) include (event, datetime);
-- время выполнения запроса после оптиизации: 0.114, что в 657к быстрее оригинального запроса

--------------------------------------------
-- топ 4 по медленности выполнения - запрос:
--------------------------------------------
-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );
-- время выполнения запроса до оптиизации: 143.239
-- анализ запроса показывает, что мы несколько раз полностью сканируем таблицу order_statuses
-- когда ищем последний статус
-- можем добавить индекс на колонку order_id, чтобы фильтрация по вложенному запросу использовала индекс
create index idx_order_statuses_order_id on order_statuses(order_id);
-- теперь полный скан по таблице статусов заказов не происходит, а используется индекс
-- мы все еще полностью сканируем таблицу statuses, но даже при добавлении на нее индекса, планировщик
-- решает не использовать его (поскольку эта таблица весьма маленькая), так что индекс тут не будет уместен
-- время выполнения запроса после оптиизации: 0.057, что в 2.5к быстрее оригинального запроса

--------------------------------------------
-- топ 5 по медленности выполнения - запрос:
--------------------------------------------
-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
	SELECT item
	FROM (SELECT item, SUM(count) AS total_sales
		  FROM order_items oi
		  GROUP BY 1) dishes_sales
	WHERE dishes_sales.total_sales > (
		SELECT SUM(t.total_sales)/ COUNT(*)
		FROM (SELECT item, SUM(count) AS total_sales
			FROM order_items oi
			GROUP BY
				1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;
-- время выполнения запроса до оптиизации: 76.565
-- тут проблема в повторяющихся подзапросах - одни и те же запросы выполняются несколько раз
-- мы можем решить эту проблему при помощи cte:
with dishes_sales as (
    SELECT item, SUM(count) AS total_sales
    FROM order_items oi
    GROUP BY 1
),
items_more_avg as (
    SELECT item
    FROM dishes_sales
    WHERE dishes_sales.total_sales > (
        SELECT SUM(dishes_sales.total_sales) / COUNT(*)
        FROM dishes_sales
    )
)
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
    JOIN dishes d ON d.object_id = oi.item
    join items_more_avg using (item)
GROUP BY 1
ORDER BY orders_quantity DESC;
-- данный запрос выполняется 56.895, что на 30% быстрее оригинального запроса