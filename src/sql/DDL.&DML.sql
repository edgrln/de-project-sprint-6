-- Шаг 2. Создать таблицу group_log в Vertica
DROP TABLE IF EXISTS LAKSHINEDYANDEXRU__STAGING.group_log;

CREATE TABLE LAKSHINEDYANDEXRU__STAGING.group_log
(
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(100),
    event_dt DATETIME
)
ORDER BY group_id, user_id, event_dt
SEGMENTED BY HASH(group_id) ALL nodes
PARTITION BY event_dt::DATE
GROUP BY calendar_hierarchy_day(event_dt::DATE,3 , 2);


-- Шаг 4. Создать таблицу связи
DROP TABLE IF EXISTS LAKSHINEDYANDEXRU__DWH.l_user_group_activity;


CREATE TABLE LAKSHINEDYANDEXRU__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES LAKSHINEDYANDEXRU__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_user_message REFERENCES LAKSHINEDYANDEXRU__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL nodes
PARTITION BY load_dt::date 
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



-- Шаг 5. Создать скрипты миграции в таблицу связи
INSERT INTO LAKSHINEDYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)

SELECT DISTINCT
        HASH(hu.hk_user_id, hg.hk_group_id)
        ,hu.hk_user_id
        ,hg.hk_group_id
        ,NOW() AS load_dt
        ,'s3' AS load_src

FROM LAKSHINEDYANDEXRU__STAGING.group_log AS gl
LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_groups AS hg ON gl.group_id = hg.group_id;


-- Шаг 6. Создать и наполнить сателлит
DROP TABLE IF EXISTS LAKSHINEDYANDEXRU__DWH.s_auth_history;

CREATE TABLE LAKSHINEDYANDEXRU__DWH.s_auth_history
(
   hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_l_user_group_activity REFERENCES LAKSHINEDYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
   user_id_from INT,
   event VARCHAR(20),
   event_dt DATETIME,
   load_dt DATETIME,
   load_src VARCHAR(20) 
)
ORDER BY event_dt
SEGMENTED BY hk_l_user_group_activity ALL nodes
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::DATE, 3, 2);


INSERT INTO LAKSHINEDYANDEXRU__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)

SELECT luga.hk_l_user_group_activity
        ,gl.user_id_from
        ,gl.event
        ,gl.event_dt
        ,NOW() AS load_dt
        ,'s3' AS load_src


FROM LAKSHINEDYANDEXRU__STAGING.group_log AS gl
LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN LAKSHINEDYANDEXRU__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id;


-- Шаг 7. Подготовить CTE для ответов бизнесу

WITH user_group_messages AS (
    SELECT lgd.hk_group_id, COUNT (DISTINCT klum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM LAKSHINEDYANDEXRU__DWH.l_groups_dialogs lgd
    JOIN LAKSHINEDYANDEXRU__DWH.l_user_message klum ON lgd.hk_message_id = klum.hk_message_id
    GROUP BY lgd.hk_group_id
)
SELECT hk_group_id,
cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10;


-- Шаг 7.2. Подготовить CTE user_group_log

WITH user_group_log AS (
    SELECT
        luga.hk_group_id
        ,count(DISTINCT luga.hk_user_id) AS cnt_added_users
        FROM LAKSHINEDYANDEXRU__DWH.s_auth_history AS sah
        LEFT JOIN LAKSHINEDYANDEXRU__DWH.l_user_group_activity luga
        ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_groups AS hg ON luga.hk_group_id = hg.hk_group_id
        WHERE sah.event = 'add'
        AND hg.hk_group_id IN (
        SELECT hk_group_id
        FROM LAKSHINEDYANDEXRU__DWH.h_groups
        ORDER BY registration_dt
        LIMIT 10
        )
        GROUP BY luga.hk_group_id
)

SELECT hk_group_id
,cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10;


-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса


WITH user_group_log AS (
    SELECT
        luga.hk_group_id
        , count(distinct luga.hk_user_id) AS cnt_added_users
        FROM LAKSHINEDYANDEXRU__DWH.s_auth_history AS sah 
        LEFT JOIN LAKSHINEDYANDEXRU__DWH.l_user_group_activity as luga
        ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        LEFT JOIN LAKSHINEDYANDEXRU__DWH.h_groups AS hg ON luga.hk_group_id = hg.hk_group_id
        WHERE sah.event = 'add'
        AND hg.hk_group_id IN (
            -- 10 самых ранних групп
            SELECT hk_group_id
            FROM LAKSHINEDYANDEXRU__DWH.h_groups
            ORDER BY registration_dt
            LIMIT 10
        )
        GROUP BY luga.hk_group_id
)
,user_group_messages AS (
    SELECT 
    lgd.hk_group_id
    ,count(distinct lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM LAKSHINEDYANDEXRU__DWH.l_user_message AS lum
    LEFT JOIN LAKSHINEDYANDEXRU__DWH.l_groups_dialogs AS lgd ON lum.hk_message_id = lgd.hk_message_id
    GROUP BY lgd.hk_group_id
)


SELECT ugl.hk_group_id
,ugl.cnt_added_users 
,ugm.cnt_users_in_group_with_messages 
,ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion 
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;
