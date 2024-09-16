CREATE SOURCE user_behaviors (
    user_id VARCHAR,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMP WITH TIME ZONE,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'user_behaviors',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW thread_view_count AS WITH t AS (
    SELECT
        target_id,
        COUNT() AS view_count,
        window_start AS window_time
    FROM
        TUMBLE(
            user_behaviors,
            event_timestamp,
            INTERVAL '10 minutes'
        )
    WHERE
        target_type = 'thread'
        AND behavior_type = 'show'
    GROUP BY
        target_id,
        window_start
)
SELECT
    target_id,
    SUM(t.view_count) AS view_count,
    window_start,
    window_end
FROM
    HOP(
        t,
        t.window_time,
        INTERVAL '10 minutes',
        INTERVAL '1440 minutes'
    )
GROUP BY
    target_id,
    window_start,
    window_end;


SELECT * FROM thread_view_count
WHERE
    window_start > ('2024-09-15 20:00Z' :: TIMESTAMP WITH TIME ZONE - INTERVAL '1 day')
    AND window_start < ('2024-09-23 23:00Z' :: TIMESTAMP WITH TIME ZONE - INTERVAL '1 day' + INTERVAL '10 minutes')
    AND target_id = 'thread97'
ORDER BY window_start DESC;