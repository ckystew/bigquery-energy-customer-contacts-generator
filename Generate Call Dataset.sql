WITH calls AS (
  SELECT
    calls.id
    ,customers.id AS customer_id
    ,agents.id AS agent_id
    ,agents.start_date as agent_start_date
    ,agents.is_first_agent
    ,agents.agent_productivity
    ,IF(
      agents.id IS NOT NULL
      ,DATE_FROM_UNIX_DATE(CAST(UNIX_DATE(agents.start_date) + (finish - UNIX_DATE(agents.start_date)) * RAND() AS INT64))
      ,DATE_FROM_UNIX_DATE(CAST(start + (finish - start) * RAND() AS INT64))
    ) AS call_start_date
    ,call_length_seconds
  FROM (
    SELECT
      id
      ,ROUND(RAND() * 600) AS call_length_seconds
      ,ROUND(RAND() * (SELECT COUNT(*) FROM `so-energy-test.generated_sources.customers`)) AS customer_id
      ,CEIL(RAND() * (SELECT COUNT(*) FROM `so-energy-test.generated_sources.agents` WHERE team = 'Phones')) AS agent_id
    FROM
      UNNEST(GENERATE_ARRAY(1, 4000)) AS id
  ) calls
  ,UNNEST([STRUCT(UNIX_DATE('2022-01-01') AS start, UNIX_DATE(CURRENT_DATE()) AS finish)])
  LEFT JOIN 
    `so-energy-test.generated_sources.customers` customers
  ON
    calls.customer_id = customers.id
  LEFT JOIN (
    SELECT
      *
      ,IF(MIN(start_date) OVER() = start_date
        ,1
        ,0
      ) AS is_first_agent
      ,ROW_NUMBER() OVER() AS rn
      ,IF(ROW_NUMBER() OVER() = 1
            ,1
            ,RAND()
        ) AS agent_productivity
    FROM
    `so-energy-test.generated_sources.agents` 
    WHERE
      team = 'Phones'
  ) agents
  ON
    calls.agent_id = agents.rn
)

SELECT
  calls.id
  ,customer_id
  ,calls.agent_id
  ,call_start_date
  ,CAST(call_length_seconds AS INT64) AS call_length_seconds
  ,CAST(
    (RAND() * 5)
    *
    CASE 
      WHEN
        call_start_date BETWEEN '2021-09-01' AND '2021-10-01' 
      THEN 
        100
      WHEN
        call_start_date BETWEEN '2022-08-01' AND '2022-09-01'
      THEN
        200
      WHEN
        call_start_date BETWEEN '2022-12-01' AND '2023-01-01'
      THEN
        80
      ELSE
        50
    END 
  AS INT64)
  AS hold_length_seconds
FROM
  calls
LEFT JOIN (
  SELECT
    agent_id
    ,COUNT(calls.id)
    ,COUNT(calls.id) / DATE_DIFF(CURRENT_DATE(), agent_start_date, DAY) AS calls_per_day
  FROM
    calls
  GROUP BY 1, agent_start_date
) rates
ON
  calls.agent_id = rates.agent_id
CROSS JOIN (
  SELECT
    COUNT(*) / DATE_DIFF(CURRENT_DATE(), MIN(agent_start_date), DAY) AS calls_per_day
  FROM
    calls
  WHERE
    is_first_agent = 1
) og_agent
WHERE
  RAND() <= og_agent.calls_per_day/rates.calls_per_day
  AND RAND() <= a.agent_productivity