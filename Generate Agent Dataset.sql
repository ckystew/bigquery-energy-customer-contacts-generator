

--SELECT CONCAT(a.name, ' ', b.name)
SELECT
  a.rn AS id
  ,CONCAT(a.name, ' ', b.name) AS name
  ,DATE_FROM_UNIX_DATE(CAST(start + (finish - start) * RAND() AS INT64)) start_date
  ,IF(RAND() < 0.3, 'Phones', 'Emails') AS team
FROM (
  SELECT 
    ROW_NUMBER() OVER() AS rn
    ,name
  FROM 
    `bigquery-public-data.usa_names.usa_1910_2013`
  WHERE RAND() < 20/5552452
) AS a
,UNNEST([STRUCT(UNIX_DATE('2022-01-01') AS start, UNIX_DATE(CURRENT_DATE()) AS finish)])
LEFT JOIN (
  SELECT 
    ROW_NUMBER() OVER() AS rn
    ,name
  FROM 
    `bigquery-public-data.usa_names.usa_1910_2013`
  WHERE RAND() < 20/5552452
) b
ON a.rn = b.rn
WHERE b.rn IS NOT NULL