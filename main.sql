-- Querry 1  : We want to find the total number of cars by model by country.

SELECT
    "Country",
    "Make",
	"Model",
    SUM("Sales_Volume") AS total_cars
FROM
    consumer_data
GROUP BY
    "Country", "Make","Model";

-- Querry B : We want to know which country has the most of each model

SELECT
	DISTINCT consumer_data."Model",
	consumer_data."Make",
	consumer_data."Country",
	sum(consumer_data."Sales_Volume") as total_sale
	
FROM consumer_data
GROUP BY 
	consumer_data."Country",
	consumer_data."Model",
	consumer_data."Make"

-- Querry C : We want to know if any model is sold in the USA but not in France.

SELECT DISTINCT
       consumer_data."Model",
       consumer_data."Make"
FROM consumer_data
WHERE consumer_data."Country" = 'USA'
  AND NOT EXISTS (
    SELECT 1
    FROM consumer_data france_data
    WHERE france_data."Country" = 'France'
      AND consumer_data."Model" = france_data."Model"
      AND consumer_data."Make" = france_data."Make"
);

-- Querry D :  We want to know how much the average car costs in every country by engine type

SELECT consumer_data."Country", car_data."Engine_Type", AVG(car_data."Price")
FROM car_data
INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                          AND car_data."Model" = consumer_data."Model"
GROUP BY 
	car_data."Engine_Type",
	consumer_data."Country"

-- Querry E : We want to know the average ratings of electric cars vsthermal cars

SELECT car_data."Engine_Type", AVG(consumer_data."Review_Score") AS avg_review_score
FROM car_data
INNER JOIN consumer_data ON car_data."Make" = consumer_data."Make"
                          AND car_data."Model" = consumer_data."Model"
GROUP BY car_data."Engine_Type";