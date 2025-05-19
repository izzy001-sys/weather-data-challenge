-- Insering year into the dim_year tables for use in the fact_weather table

INSERT INTO dim_year (year)
SELECT DISTINCT EXTRACT(YEAR FROM d.full_date)::INT
FROM fact_weather fw
JOIN dim_date d ON fw.date_id = d.date_id
ON CONFLICT (year) DO NOTHING;


