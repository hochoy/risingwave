CREATE SOURCE prometheus (
    labels STRUCT <
        __name__ VARCHAR,
        instance VARCHAR,
        job VARCHAR
    >,
    name VARCHAR,
    timestamp TIMESTAMP WITH TIME ZONE,
    value VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'prometheus',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW metric_avg_30s AS
SELECT
    name AS metric_name,
    window_start AS metric_time,
    avg(value :: decimal) AS metric_value
FROM
    tumble(
        prometheus,
        timestamp,
        interval '30 s'
    )
GROUP BY
    name,
    window_start
ORDER BY
    window_start;