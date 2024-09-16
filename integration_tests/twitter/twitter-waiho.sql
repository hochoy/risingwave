CREATE SOURCE twitter (
    data STRUCT <
        created_at TIMESTAMP WITH TIME ZONE,
        id VARCHAR,
        text VARCHAR,
        lang VARCHAR
    >,
    author STRUCT <
        created_at TIMESTAMP WITH TIME ZONE,
        id VARCHAR,
        name VARCHAR,
        username VARCHAR,
        followers INT
    >
) WITH (
    connector = 'kafka',
    topic = 'twitter',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW hot_hashtags AS
WITH tags AS (
    SELECT
        unnest(regexp_matches((data).text, '#\w+', 'g')) AS hashtag,
        (data).created_at AS created_at
    FROM
        twitter
)
SELECT 
    hashtag,
    COUNT(*) AS hashtag_occurences,
    window_start
FROM
    TUMBLE(
        tags,
        created_at,
        INTERVAL '1 day'
    )
GROUP BY
    hashtag,
    window_start;