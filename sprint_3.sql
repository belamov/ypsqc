create database sprint_3;

--/usr/bin/pg_restore --dbname=sprint_3 --username=postgres --host=localhost --port=5433 /home/belamov/Downloads/practicum_sql_for_dev_project_3.sql

-- Напишите хранимую процедуру update_employees_rate, которая обновляет почасовую ставку сотрудников на определённый процент.
-- При понижении ставка не может быть ниже минимальной — 500 рублей в час.
-- Если по расчётам выходит меньше, устанавливают минимальную ставку.
-- На вход процедура принимает строку в формате json:
-- [
--     -- uuid сотрудника                                      процент изменения ставки
--     {"employee_id": "6bfa5e20-918c-46d0-ab18-54fc61086cba", "rate_change": 10},
--     -- -- --
--     {"employee_id": "5a6aed8f-8f53-4931-82f4-66673633f2a8", "rate_change": -5}
-- ]
create or replace procedure update_employees_rate(data json)
language plpgsql
as $$
    declare
        _em json;
begin
    for _em in
        select * from json_array_elements(data)
    loop
       update employees
       set
        rate = case
            when rate/100*(100 + (_em->>'rate_change')::integer) < 500 then 500
            else rate/100*(100 + (_em->>'rate_change')::integer)
        end
       where
           id = (_em->>'employee_id')::uuid;
    end loop;
end;
$$;

CALL update_employees_rate(
    '[
        {"employee_id": "80718590-e2bf-492b-8c83-6f8c11d007b1", "rate_change": 10},
        {"employee_id": "f0e2ca99-3863-4cbf-a308-1939195d0df8", "rate_change": -5}
    ]'::json
);

-- Напишите хранимую процедуру indexing_salary, которая повышает зарплаты всех сотрудников на определённый процент.
-- Процедура принимает один целочисленный параметр — процент индексации p.
-- Сотрудникам, которые получают зарплату по ставке ниже средней относительно всех сотрудников до индексации, начисляют
-- дополнительные 2% (p + 2). Ставка остальных сотрудников увеличивается на p%.
-- Зарплата хранится в БД в типе данных integer, поэтому если в результате повышения зарплаты образуется
-- дробное число, его нужно округлить до целого.
create or replace procedure indexing_salary(p integer)
language plpgsql
as $$
declare
    _avg_salary numeric;
begin
    select avg(rate) from employees
    into _avg_salary;

    update employees
        set rate = case
            when rate<_avg_salary then round(rate/100*(102+p), 0)
            else round(rate/100*(100+p), 0)
            end;
end;
$$;

CALL indexing_salary(22);

-- Завершая проект, нужно сделать два действия в системе учёта:
--   - Изменить значение поля is_active в записи проекта на false — чтобы рабочее время по этому проекту больше не учитывалось.
--   - Посчитать бонус, если он есть — то есть распределить неизрасходованное время между всеми членами команды проекта.
--     Неизрасходованное время — это разница между временем, которое выделили на проект (estimated_time), и фактически потраченным.
--     Если поле estimated_time не задано, бонусные часы не распределятся.
--     Если отработанных часов нет — расчитывать бонус не нужно.
--
-- Разберёмся с бонусом.
-- Если в момент закрытия проекта estimated_time:
--   - не NULL,
--   - больше суммы всех отработанных над проектом часов,
-- всем членам команды проекта начисляют бонусные часы.
-- Размер бонуса считают так: 75% от сэкономленных часов делят на количество участников проекта, но не более 16 бонусных часов на сотрудника.
-- Дробные значения округляют в меньшую сторону (например, 3.7 часа округляют до 3).
-- Рабочие часы заносят в логи с текущей датой.
-- Например, если на проект запланировали 100 часов, а сделали его за 30 — 3/4 от сэкономленных 70 часов распределят бонусом между участниками проекта.
-- Создайте пользовательскую процедуру завершения проекта close_project.
-- Если проект уже закрыт, процедура должна вернуть ошибку без начисления бонусных часов.
create or replace procedure close_project(p_project_id uuid)
language plpgsql
as $$
declare
    _unspent_time integer;
    _spent_time integer;
    _is_project_actve bool;
    _estimated_time integer;
    _bonus_hours_for_employee integer;
    _employees_in_project integer;
begin
    select is_active
    into _is_project_actve
    from projects
    where id = p_project_id;

    if not _is_project_actve then
        raise exception 'project already closed';
    end if;

    update projects
    set is_active = false
    where id = p_project_id;

    select estimated_time
    into _estimated_time
    from projects
    where id = p_project_id;

    if _estimated_time is null then
        return;
    end if;

    select sum(work_hours), count(distinct employee_id)
    into _spent_time, _employees_in_project
    from logs
    where project_id = p_project_id;

    if _spent_time = 0 then
        return;
    end if;

    if _estimated_time < _spent_time then
        return;
    end if;

    _unspent_time := (_estimated_time - _spent_time) * 0.75;

    _bonus_hours_for_employee := trunc(_unspent_time/_employees_in_project);
    if _bonus_hours_for_employee > 16 then
        _bonus_hours_for_employee = 16;
    end if;

    insert into logs (employee_id, project_id, work_date, work_hours)
    select distinct employee_id, p_project_id, current_timestamp, _bonus_hours_for_employee
    from logs
    where project_id = p_project_id;
end;
$$;

CALL close_project('4abb5b99-3889-4c20-a575-e65886f266f9');

-- Напишите процедуру log_work для внесения отработанных сотрудниками часов.
-- Процедура добавляет новые записи о работе сотрудников над проектами.
-- Процедура принимает id сотрудника, id проекта, дату и отработанные часы и вносит данные в таблицу logs.
-- Если проект завершён, добавить логи нельзя — процедура должна вернуть ошибку Project closed.
-- Количество залогированных часов может быть в этом диапазоне: от 1 до 24 включительно — нельзя внести менее 1 часа или больше 24.
-- Если количество часов выходит за эти пределы, необходимо вывести предупреждение о недопустимых данных и остановить выполнение процедуры.
-- Запись помечается флагом required_review, если:
-- залогированно более 16 часов за один день — Dream Big заботится о здоровье сотрудников;
-- запись внесена будущим числом;
-- запись внесена более ранним числом, чем на неделю назад от текущего дня — например, если сегодня 10.04.2023, все записи старше 3.04.2023 получат флажок.
create or replace procedure log_work(p_employee_uuid uuid, p_project_uuid uuid, p_work_date date, p_worked_hours integer)
language plpgsql
as $$
declare
    _project_active bool;
    _required_review bool;
begin
    select is_active
    into _project_active
    from projects
    where id = p_project_uuid;

    if not _project_active then
        raise exception 'Project closed';
    end if;

    if p_worked_hours < 1 or p_worked_hours > 24 then
        raise exception 'worked hours must be betwwwn 1 and 24';
    end if;

    _required_review := false;

    if p_worked_hours > 16 then
        _required_review := true;
    end if;

    if p_work_date > current_date then
        _required_review := true;
    end if;

    if p_work_date < current_date - '7 days'::interval then
        _required_review := true;
    end if;

    insert into logs (employee_id, project_id, work_date, work_hours, required_review)
    values (p_employee_uuid, p_project_uuid, p_work_date, p_worked_hours, _required_review);
end;
$$;

CALL log_work(
    'b15bb4c0-1ee1-49a9-bc58-25a014eebe36', -- employee uuid
    '7164736e-af27-49b8-aec2-183fe85d0295', -- project uuid
    '2023-10-22',                           -- work date
    4                                       -- worked hours
);

-- Чтобы бухгалтерия корректно начисляла зарплату, нужно хранить историю изменения почасовой ставки сотрудников.
-- Создайте отдельную таблицу employee_rate_history с такими столбцами:
-- id — id записи,
-- employee_id — id сотрудника,
-- rate — почасовая ставка сотрудника,
-- from_date — дата назначения новой ставки.
-- Внесите в таблицу текущие данные всех сотрудников.
-- В качестве from_date используйте дату основания компании: '2020-12-26'.
-- Напишите триггерную функцию save_employee_rate_history и триггер change_employee_rate.
-- При добавлении сотрудника в таблицу employees и изменении ставки сотрудника триггер автоматически
-- вносит запись в таблицу employee_rate_history из трёх полей: id сотрудника, его ставки и текущей даты.
create table employee_rate_history(
    id serial primary key,
    employee_id uuid references employees (id),
    rate integer,
    from_date date
);

insert into employee_rate_history (employee_id, rate, from_date)
select id, rate, '2020-12-26'
from employees;

create or replace function save_employee_rate_history()
returns trigger
language plpgsql
as $$
begin
    if old.rate is distinct from new.rate then
        insert into employee_rate_history (employee_id, rate, from_date)
        values (new.id, new.rate, current_timestamp);
    end if;
    return new;
end;
$$;

create or replace trigger change_employee_rate
after update or insert on employees
for each row
execute function save_employee_rate_history();

-- После завершения каждого проекта Dream Big проводит корпоративную вечеринку, чтобы отпраздновать
-- очередной успех и поощрить сотрудников.
-- Тех, кто посвятил проекту больше всего часов, награждают премией «Айтиголик» — они получают
-- почётные грамоты и ценные подарки от заказчика.
-- Чтобы вычислить айтиголиков проекта, напишите функцию best_project_workers.
-- Функция принимает id проекта и возвращает таблицу с именами трёх сотрудников, которые
-- залогировали максимальное количество часов в этом проекте.
-- Результирующая таблица состоит из двух полей: имени сотрудника и количества часов, отработанных на проекте.
create or replace function best_project_workers(p_project_uuid uuid)
returns table (employee text, work_hours bigint)
language plpgsql
as $$
begin
    return query
    select e.name, sum(l.work_hours) as work_hours
    from employees e
    join logs l on l.employee_id=e.id
    where project_id = p_project_uuid
    group by e.name
    order by work_hours desc
    limit 3;
end;
$$;

select employee, work_hours from best_project_workers(
    '4abb5b99-3889-4c20-a575-e65886f266f9' -- Project UUID
);

-- Напишите для бухгалтерии функцию calculate_month_salary для расчёта зарплаты за месяц.
-- Функция принимает в качестве параметров даты начала и конца месяца и возвращает результат в виде
-- таблицы с четырьмя полями: id (сотрудника), employee (имя сотрудника), worked_hours и salary.
-- Процедура суммирует все залогированные часы за определённый месяц и умножает на актуальную почасовую ставку сотрудника.
-- Исключения — записи с флажками required_review и is_paid.
-- Если суммарно по всем проектам сотрудник отработал более 160 часов в месяц, все часы свыше 160 оплатят с коэффициентом 1.25.
create or replace function calculate_month_salary(p_date_from date, p_date_to date)
returns table (id uuid, employee text, worked_hours bigint, salary numeric)
language plpgsql
as $$
begin
    return query
    select
        e.id,
        e.name,
        sum(l.work_hours),
        case
            when sum(l.work_hours)>160 then (160*e.rate) + (sum(l.work_hours)-160)*(e.rate*1.25)
            else sum(l.work_hours)*e.rate
        end
    from logs l
    join employees e on l.employee_id = e.id
    where work_date between p_date_from and p_date_to
        and l.is_paid = false
        and l.required_review = false
    group by e.id, e.name;
end;
$$;

select * from calculate_month_salary(
    '2023-10-01',  -- start of month
    '2023-10-31'   -- end of month
);