-- Query 1  : We want to find the total number of cars by model by country.

SELECT
    "Country",
    "Make",
	"Model",
    SUM("Sales_Volume") AS total_cars
FROM
    consumer_data
GROUP BY
    "Country", "Make","Model";

-- Query B : We want to know which country has the most of each model

WITH SALES_VOLUME AS (
  SELECT consumer_data."Country", consumer_data."Make", consumer_data."Model", sum(consumer_data."Sales_Volume") as SV
  FROM consumer_data
  GROUP BY "Country", "Make", "Model"
  ORDER BY "Make", "Model", SV DESC
),
ROW_NUMBER_SALES_VOLUME AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY "Make", "Model" ORDER BY SV DESC) AS row_number
  FROM SALES_VOLUME
)
SELECT *
FROM ROW_NUMBER_SALES_VOLUME
WHERE "row_number" = 1

-- Query C : We want to know if any model is sold in the USA but not in France.

SELECT DISTINCT
       us_data."Model",
       us_data."Make"
FROM consumer_data as us_data
WHERE us_data."Country" = 'USA'
  AND NOT EXISTS (
    SELECT 1
    FROM consumer_data as france_data
    WHERE france_data."Country" = 'France'
      AND us_data."Model" = france_data."Model"
      AND us_data."Make" = france_data."Make"
);

-- Query D :  We want to know how much the average car costs in every country by engine type

SELECT consumer_data."Country", car_data."Engine_Type", AVG(car_data."Price")
FROM car_data
INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                          AND car_data."Model" = consumer_data."Model"
GROUP BY 
	car_data."Engine_Type",
	consumer_data."Country"

-- Query E : We want to know the average ratings of electric cars vs thermal cars

SELECT car_data."Engine_Type", AVG(consumer_data."Review_Score") AS avg_review_score
FROM car_data
INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                          AND car_data."Model" = consumer_data."Model"
GROUP BY car_data."Engine_Type";