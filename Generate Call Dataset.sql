WITH chains AS (
  SELECT
    emails.id
    ,IF(agent_responses = 0, 1, agent_responses) AS agent_responses
    ,IF(agent_responses <= 1, 0, agent_responses - 1) AS customer_responses
    ,customers.id AS customer_id
    ,agents.id AS agent_id
    ,agents.start_date as agent_start_date
    ,agents.is_first_agent
    ,IF(
      agents.id IS NOT NULL
      ,DATE_FROM_UNIX_DATE(CAST(UNIX_DATE(agents.start_date) + (finish - UNIX_DATE(agents.start_date)) * RAND() AS INT64))
      ,DATE_FROM_UNIX_DATE(CAST(start + (finish - start) * RAND() AS INT64))
    ) AS chain_start_date
  FROM (
    SELECT
      id
      ,ROUND(RAND() * 5) AS agent_responses
      ,ROUND(RAND() * ((SELECT COUNT(*) FROM `so-energy-test.generated_sources.customers`))) AS customer_id -- Select more than enough customers to guarantee good amount of nulls
      ,CEIL(RAND() * (SELECT COUNT(*) FROM `so-energy-test.generated_sources.agents` WHERE team = 'Emails')) AS agent_id
    FROM
      UNNEST(GENERATE_ARRAY(1, 3000)) AS id
  ) emails
  ,UNNEST([STRUCT(UNIX_DATE('2022-01-01') AS start, UNIX_DATE(CURRENT_DATE()) AS finish)])
  LEFT JOIN 
    `so-energy-test.generated_sources.customers` customers
  ON
    emails.customer_id = customers.id
  LEFT JOIN (
    SELECT
      *
      ,ROW_NUMBER() OVER() AS rn
      ,IF(MIN(start_date) OVER() = start_date
        ,1
        ,0
      ) AS is_first_agent
    FROM
    `so-energy-test.generated_sources.agents` 
    WHERE
      team = 'Emails'
  ) agents
  ON
    emails.agent_id = agents.rn
  --WHERE
    -- Remove roughly 50% of customers at random
    -- customers.id NOT IN (
    --   SELECT
    --     id
    --   FROM
    --     `so-energy-test.generated_sources.customers`
    --   WHERE
    --     RAND() < 0.5
    -- )

)
SELECT
  chains.id
  ,agent_responses
  ,customer_responses
  ,customer_id
  ,chains.agent_id
  ,chain_start_date
  ,DATE_ADD(chain_start_date, INTERVAL CAST(ROUND(RAND() * 5) AS INT64) DAY) as chain_end_date
FROM
  chains
CROSS JOIN (
  SELECT
    COUNT(*) / DATE_DIFF(CURRENT_DATE(), MIN(chains.agent_start_date), DAY) AS calls_per_day
  FROM
    chains
  WHERE
    is_first_agent = 1
) og_agent
LEFT JOIN (
    SELECT 
        agent_id
        ,COUNT(*) / DATE_DIFF(CURRENT_DATE(), MIN(chains.agent_start_date), DAY) AS calls_per_day
    FROM
        chains
    GROUP BY 1, chains.agent_start_date
) rates
ON
    chains.agent_id = rates.agent_id
WHERE
  RAND() <= og_agent.calls_per_day/rates.calls_per_day