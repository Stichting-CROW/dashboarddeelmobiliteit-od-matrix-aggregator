SELECT *
FROM od_h3
WHERE origin_cell = 608433150917345279
AND aggregation_period_id IN (
    SELECT aggregation_period_id 
    FROM od_h3_aggregation_period
    WHERE extract(isodow from start_time_period) IN (1, 2, 3, 4, 5)
    AND extract(hour from start_time_period) IN (2, 4, 10)
    AND start_time_period >= '2023-01-01' and end_time_period <= '2023-01-05'
);
SELECT start_time_period
    FROM od_h3_aggregation_period
    WHERE extract(isodow from start_time_period) IN (1, 2, 3, 4, 5)
    AND extract(hour from start_time_period) IN (2, 4, 10)
    AND start_time_period >= '2023-01-01' and end_time_period <= '2023-01-05'


SELECT destination_cell, sum(number_of_trips)
FROM od_h3
WHERE origin_cell IN (608433150917345279, 608433156034396159, 608433155967287295)
AND modality = 'moped'
AND aggregation_period_id IN (
    SELECT aggregation_period_id 
    FROM od_h3_aggregation_period
    WHERE extract(isodow from start_time_period) IN (1, 2, 3, 4, 5)
    AND extract(hour from start_time_period) IN (2, 4, 10)
    AND start_time_period >= '2022-11-01' and end_time_period <= '2023-02-07'
) GROUP by destination_cell order by sum(number_of_trips) DESC;

SELECT destination_cell, origin_cell, sum(number_of_trips)
FROM od_h3
WHERE (origin_cell IN (608433150917345279, 608433156034396159, 608433155967287295, 608433151034785791,
 608433150850236415,
 608433154876768255,
 608433151135449087,
 608433143266934783,
 608433155195535359,
 608433150783127551,
 608433154776104959,
 608433156034396159,
 608433155078094847,
 608433157477236735,
 608542612973944831,
 608433154641887231,
 608433155749183487,
 608433150967676927)
or destination_cell IN (608433150917345279, 608433156034396159, 608433155967287295,
 608433150850236415,
 608433154876768255,
 608433151135449087,
 608433143266934783,
 608433155195535359,
 608433150783127551,
 608433154776104959,
 608433156034396159,
 608433155078094847,
 608433157477236735,
 608542612973944831,
 608433154641887231,
 608433155749183487,
 608433150967676927))
AND modality = 'moped'
AND aggregation_period_id IN (
    SELECT aggregation_period_id 
    FROM od_h3_aggregation_period
    WHERE extract(isodow from start_time_period) IN (1, 2, 3, 4, 5)
    AND extract(hour from start_time_period) IN (2, 4, 10)
    AND start_time_period >= '2022-10-01' and end_time_period <= '2023-02-07'
) GROUP by destination_cell, origin_cell;


