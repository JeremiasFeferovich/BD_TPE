


CREATE OR REPLACE FUNCTION ReporteConsolidado(n INT) RETURNS VOID AS $$
DECLARE
    first_year INTEGER;
    last_year INTEGER;
    cCurrent_year INTEGER;
    cState TEXT;
    cGender_code TEXT;
    cEducation_level_code INTEGER;
    education_level_name TEXT;
    total_births BIGINT;
    avg_age INTEGER;
    min_age INTEGER;
    max_age INTEGER;
    avg_weight DECIMAL(4,3);
    min_weight DECIMAL(4,3);
    max_weight DECIMAL(4,3);
BEGIN
    -- Get the first year in the table
    SELECT MIN(YEAR_NUMBER) INTO first_year FROM YEAR_DATA;

    -- Check if there is data in the table
    IF first_year IS NULL THEN
        RETURN;
    END IF;

    -- Set the last year based on the number of years to show
    last_year := first_year + n - 1;

    -- Print the report header
    RAISE INFO '---------------------------------------------------------------------------------------------------------';
    RAISE INFO '----------------------------------------CONSOLIDATED BIRTH REPORT----------------------------------------';
    RAISE INFO '---------------------------------------------------------------------------------------------------------';
    RAISE INFO 'Year Category                                    Total AvgAge MinAge MaxAge AvgWeight MinWeight MaxWeight';

    -- Iterate over each year
    FOR cCurrent_year IN first_year..last_year LOOP
        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT
            SUM(BIRTHS),
        INTO
            total_births
        FROM
            US_BIRTHS_VIEW
        WHERE
            YEAR = cCurrent_year;

        -- Check if there is data for the year
        IF total_births IS NOT NULL THEN
            -- Iterate over each state with more than 200,000 births
            FOR cState IN (
                SELECT
                    STATE
                FROM
                    US_BIRTHS_VIEW
                WHERE
                    YEAR = cCurrent_year
                GROUP BY
                    STATE
                HAVING
                    SUM(BIRTHS) > 200000
                ORDER BY
                    STATE DESC
            ) LOOP
                -- Get the metrics for the current state and year
                SELECT
                        SUM(BIRTHS),
                        CAST(ROUND(AVG(MOTHER_AVERAGE_AGE)) AS INTEGER),
                        CAST(ROUND(MIN(MOTHER_AVERAGE_AGE)) AS INTEGER),
                        CAST(ROUND(MAX(MOTHER_AVERAGE_AGE)) AS INTEGER),
                        CAST(AVG(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                        CAST(MIN(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                        CAST(MAX(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3))    
                INTO
                    total_births,
                    avg_age,
                    min_age,
                    max_age,
                    avg_weight,
                    min_weight,
                    max_weight
                FROM
                    US_BIRTHS_VIEW
                WHERE
                    YEAR = cCurrent_year
                    AND STATE = cState;

                -- Print the state and corresponding metrics
                RAISE INFO '---- % % % % % % % % %',
                    'State: ',
                    FORMAT('%-60s', cState),
                    FORMAT('%-10s', total_births),
                    FORMAT('%-10s', avg_age),
                    FORMAT('%-10s', min_age),
                    FORMAT('%-10s', max_age),
                    FORMAT('%-10s', avg_weight),
                    FORMAT('%-10s', min_weight),
                    FORMAT('%-10s', max_weight);

                END LOOP;
                    

        -- Iterate over each gender category
        FOR cGender_code IN (
            SELECT
                GENDER
            FROM
                US_BIRTHS_VIEW
            WHERE
                YEAR = cCurrent_year
            GROUP BY
                GENDER
            ORDER BY
                GENDER DESC
        ) LOOP
            -- Get the metrics for the current gender and year
            SELECT
                SUM(BIRTHS),
                CAST(ROUND(AVG(MOTHER_AVERAGE_AGE)) AS INTEGER),
                CAST(ROUND(MIN(MOTHER_AVERAGE_AGE)) AS INTEGER),
                CAST(ROUND(MAX(MOTHER_AVERAGE_AGE)) AS INTEGER),
                CAST(AVG(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                CAST(MIN(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                CAST(MAX(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3))
            INTO
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight
            FROM
                US_BIRTHS_VIEW
            WHERE
                YEAR = cCurrent_year
                AND GENDER = cGender_code;

            -- Print the gender and corresponding metrics
            RAISE INFO '---- % % % % % % % % % %',
                'Gender: ',
                FORMAT('%-60s', cGender_code),
                FORMAT('%-10s', total_births),
                FORMAT('%-10s', avg_age),
                FORMAT('%-10s', min_age),
                FORMAT('%-10s', max_age),
                FORMAT('%-10s', avg_weight),
                FORMAT('%-10s', min_weight),
                FORMAT('%-10s', max_weight);
        END LOOP;

        -- Iterate over each education level category
        FOR cEducation_level_code IN (
            SELECT EDUCATION_LEVEL_CODE FROM US_BIRTHS_VIEW
            WHERE YEAR_NUMBER = cCurrent_year
                AND EDUCATION_LEVEL_CODE <> -9
        ) LOOP
            -- Get the metrics for the current education level and year
            SELECT
                SUM(BIRTHS),
                CAST(ROUND(AVG(MOTHER_AVG_AGE)) AS INTEGER),
                CAST(ROUND(MIN(MOTHER_AVG_AGE)) AS INTEGER),
                CAST(ROUND(MAX(MOTHER_AVG_AGE)) AS INTEGER),
                CAST(AVG(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                CAST(MIN(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                CAST(MAX(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
                EDUCATION_LEVEL
            INTO
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight,
                education_level_name
            FROM
                US_BIRTHS_VIEW
            WHERE
                YEAR = current_year AND EDUCATION_LEVEL_CODE = cEducation_level_code;

            -- Print the education level and corresponding metrics
            RAISE INFO '---- % % % % % % % % %',
                'Education: ',
                FORMAT('%-60s', education_level_name),
                FORMAT('%-10s', total_births),
                FORMAT('%-10s', avg_age),
                FORMAT('%-10s', min_age),
                FORMAT('%-10s', max_age),
                FORMAT('%-10s', avg_weight),
                FORMAT('%-10s', min_weight),
                FORMAT('%-10s', max_weight);
        END LOOP;

        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT
            SUM(BIRTHS),
            CAST(ROUND(AVG(MOTHER_AVG_AGE)) AS INTEGER),
            CAST(ROUND(MIN(MOTHER_AVG_AGE)) AS INTEGER),
            CAST(ROUND(MAX(MOTHER_AVG_AGE)) AS INTEGER),
            CAST(AVG(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
            CAST(MIN(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3)),
            CAST(MAX(AVERAGE_BIRTH_WEIGHT)/1000 AS DECIMAL(4,3))
        INTO
            total_births,
            avg_age,
            min_age,
            max_age,
            avg_weight,
            min_weight,
            max_weight
        FROM
            US_BIRTHS_VIEW
        WHERE
            YEAR = cCurrent_year;

        -- Print the total metrics for the year
        RAISE INFO '---- Total % % % % % % % %',
                FORMAT('%-60s', ''),
                FORMAT('%-10s', total_births),
                FORMAT('%-10s', avg_age),
                FORMAT('%-10s', min_age),
                FORMAT('%-10s', max_age),
                FORMAT('%-10s', avg_weight),
                FORMAT('%-10s', min_weight),
                FORMAT('%-10s', max_weight);
    END IF;
    RAISE NOTICE '---------------------------------------------------------------------------------------------------------';
END LOOP;
RAISE NOTICE '-------------------------------------------------------------------------------------------------------------';
    END;
$$ LANGUAGE plpgsql;
