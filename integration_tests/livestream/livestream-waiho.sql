CREATE SOURCE live_stream_metrics (
    client_ip VARCHAR,
    user_agent VARCHAR,
    user_id VARCHAR,
    room_id VARCHAR,
    video_bps BIGINT,
    video_fps BIGINT,
    video_rtt BIGINT,
    video_lost_pps BIGINT,
    video_total_freeze_duration BIGINT,
    report_timestamp TIMESTAMP WITH TIME ZONE,
    country VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'live_stream_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW live_video_qos_10min AS
SELECT
    window_start AS report_ts,
    room_id,
    SUM(video_total_freeze_duration) AS video_total_freeze_duration,
    AVG(video_lost_pps) AS video_lost_pps,
    AVG(video_rtt) AS video_rtt
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '10' MINUTE
    )
GROUP BY
    window_start,
    room_id;

SELECT * FROM live_video_qos_10min ORDER BY room_id, report_ts;

CREATE MATERIALIZED VIEW total_user_visit_1min AS
SELECT
    window_start AS report_ts,
    COUNT(DISTINCT user_id) AS uv
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '1' MINUTE
    )
GROUP BY
    window_start;

SELECT * FROM total_user_visit_1min ORDER BY report_ts;


CREATE MATERIALIZED VIEW room_user_visit_1min AS
SELECT
    window_start AS report_ts,
    COUNT(DISTINCT user_id) AS uv,
    room_id
FROM
    TUMBLE(
        live_stream_metrics,
        report_timestamp,
        INTERVAL '1' MINUTE
    )
GROUP BY
    window_start,
    room_id;