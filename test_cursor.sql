/*
CREATE TYPE us_births_data AS (
    births BIGINT,
    avg_mother_average_age INTEGER,
    min_mother_average_age INTEGER,
    max_mother_average_age INTEGER,
    avg_average_birth_weight DECIMAL(4, 3),
    min_average_birth_weight DECIMAL(4, 3),
    max_average_birth_weight DECIMAL(4, 3)
);
*/

CREATE OR REPLACE FUNCTION PrintMetrics(pData us_births_data, pCategory TEXT) RETURNS VOID AS
$$
DECLARE
BEGIN

    -- Print the state and corresponding metrics
    RAISE INFO '% % % % % % % %',
        pCategory,
        FORMAT('%-10s', pData.births),
        FORMAT('   %-10s', pData.avg_mother_average_age),
        FORMAT('%-10s', pData.min_mother_average_age),
        FORMAT('%-10s', pData.max_mother_average_age),
        FORMAT('%-10s', pData.avg_average_birth_weight),
        FORMAT('%-10s', pData.min_average_birth_weight),
        FORMAT('%-10s', pData.max_average_birth_weight);

END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION ReportStates(pYear INT) RETURNS VOID AS
$$
DECLARE
    rState  US_BIRTHS_VIEW.state%TYPE;
    cState CURSOR FOR
        SELECT STATE
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
        GROUP BY STATE
        HAVING SUM(BIRTHS) > 200000
        ORDER BY STATE DESC;
    pData us_births_data;
    isFirst BOOLEAN := TRUE;
BEGIN
    OPEN cState;
    LOOP
        FETCH NEXT FROM cState INTO rState;
        EXIT WHEN NOT FOUND;

        SELECT SUM(births),
               CAST(ROUND(AVG(mother_average_age)) AS INTEGER),
               CAST(ROUND(MIN(mother_average_age)) AS INTEGER),
               CAST(ROUND(MAX(mother_average_age)) AS INTEGER),
               CAST(AVG(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MIN(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MAX(average_birth_weight) / 1000 AS DECIMAL(4, 3))
        INTO pData
        FROM US_BIRTHS_VIEW
        WHERE state = rState AND year = pYear;

        IF isFirst THEN
            PERFORM PrintMetrics(pData, FORMAT('%s %-80s', pYear, FORMAT('State: %s', rState)));
        ELSE
            PERFORM PrintMetrics(pData, FORMAT('---- %-80s', FORMAT('State: %s', rState)));
        END IF;
        isFirst := FALSE;

    END LOOP;
    CLOSE cState;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ReportGender(pYear INT) RETURNS VOID AS
$$
DECLARE
    rGender US_BIRTHS_VIEW.gender%TYPE;
    cGender CURSOR FOR
        SELECT GENDER
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
        GROUP BY GENDER
        ORDER BY GENDER DESC;
    pData us_births_data;
BEGIN
    OPEN cGender;
    LOOP
        FETCH NEXT FROM cGender INTO rGender;
        EXIT WHEN NOT FOUND;

        SELECT SUM(births),
               CAST(ROUND(AVG(mother_average_age)) AS INTEGER),
               CAST(ROUND(MIN(mother_average_age)) AS INTEGER),
               CAST(ROUND(MAX(mother_average_age)) AS INTEGER),
               CAST(AVG(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MIN(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MAX(average_birth_weight) / 1000 AS DECIMAL(4, 3))
        INTO pData
        FROM US_BIRTHS_VIEW
        WHERE GENDER = rGender AND YEAR = pYear;

        PERFORM PrintMetrics(pData, FORMAT('---- %-80s', FORMAT('Gender: %s', CASE
                                                                                  WHEN rGender = 'F' THEN 'Female'
                                                                                  ELSE 'Male' END)));

    END LOOP;
    CLOSE cGender;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ReportEducationLevel(pYear INT) RETURNS VOID AS
$$
DECLARE
    rEducation_level US_BIRTHS_VIEW.EDUCATION_LEVEL_CODE%TYPE;
    cEducation_level CURSOR FOR
        SELECT EDUCATION_LEVEL_CODE
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
          AND EDUCATION_LEVEL_CODE <> -9
        GROUP BY EDUCATION_LEVEL_CODE
        ORDER BY EDUCATION_LEVEL_CODE ASC;
    pData us_births_data;
BEGIN
    OPEN cEducation_level;
    LOOP
        FETCH NEXT FROM cEducation_level INTO rEducation_level;
        EXIT WHEN NOT FOUND;

        -- Get the metrics for the current education level and year
        SELECT SUM(births),
               CAST(ROUND(AVG(mother_average_age)) AS INTEGER),
               CAST(ROUND(MIN(mother_average_age)) AS INTEGER),
               CAST(ROUND(MAX(mother_average_age)) AS INTEGER),
               CAST(AVG(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MIN(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MAX(average_birth_weight) / 1000 AS DECIMAL(4, 3))
        INTO pData
        FROM US_BIRTHS_VIEW
        WHERE education_level_code = rEducation_level
          AND YEAR = pYear;

        PERFORM PrintMetrics(pData, FORMAT('---- %-80s', FORMAT('Education: %s', (SELECT mother_education_level
                                                                                  FROM EDUCATION_LEVEL
                                                                                  WHERE education_level_code = rEducation_level))));

    END LOOP;
    CLOSE cEducation_level;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ReporteConsolidado(n INT) RETURNS VOID AS
$$
DECLARE
    first_year US_BIRTHS_VIEW.year%TYPE;
    last_year US_BIRTHS_VIEW.year%TYPE;
    cYear CURSOR FOR
        SELECT YEAR
        FROM YEAR_DATA
        WHERE YEAR BETWEEN first_year AND last_year;
    rYear US_BIRTHS_VIEW.year%TYPE;
    pData us_births_data;
BEGIN
    SELECT MIN(YEAR) INTO first_year FROM YEAR_DATA;

    IF first_year IS NULL THEN
        RETURN;
    END IF;

    last_year := first_year + n - 1;

    IF last_year > (SELECT MAX(YEAR) FROM YEAR_DATA) THEN
        last_year := (SELECT MAX(YEAR) FROM YEAR_DATA);
    END IF;

    IF last_year - first_year + 1 = 0 THEN
        RETURN;
    END IF;

    -- Print the report header
    RAISE INFO '%', LPAD(' ', 165, '-');
    RAISE INFO '%', RPAD(LPAD('CONSOLIDATED BIRTH REPORT', 94, '-'), 164, '-');
    RAISE INFO '%', LPAD(' ', 165, '-');
    RAISE INFO '% % % % % % % % %',
        FORMAT('%s', 'Year'),
        FORMAT('%-80s', 'Category'),
        FORMAT('%-10s', 'Total'),
        FORMAT('%-10s', 'Avg Age'),
        FORMAT('%-10s', 'Min Age'),
        FORMAT('%-10s', 'Max Age'),
        FORMAT('%-10s', 'Avg Weight'),
        FORMAT('%-10s', 'Min Weight'),
        FORMAT('%-10s', 'Max Weight');
    RAISE INFO '%', LPAD(' ', 165, '-');

    OPEN cYear;
    LOOP
        FETCH NEXT FROM cYear INTO rYear;
        EXIT WHEN NOT FOUND;

        -- Iterate over each state with more than 200,000 births
        PERFORM ReportStates(rYear);

        -- Iterate over each gender category
        PERFORM ReportGender(rYear);

        -- Iterate over each education level category
        PERFORM ReportEducationLevel(rYear);

        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT SUM(births),
               CAST(ROUND(AVG(mother_average_age)) AS INTEGER),
               CAST(ROUND(MIN(mother_average_age)) AS INTEGER),
               CAST(ROUND(MAX(mother_average_age)) AS INTEGER),
               CAST(AVG(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MIN(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MAX(average_birth_weight) / 1000 AS DECIMAL(4, 3))
        INTO
            pData
        FROM us_births_view
        WHERE year = rYear;

        -- Print the total metrics for the year
        PERFORM PrintMetrics(pData, RPAD('-', 85,'-'));

        RAISE INFO '%', LPAD(' ', 165, '-');
    END LOOP;
    CLOSE cYear;

    RAISE INFO '%', LPAD(' ', 165, '-');
END;
$$ LANGUAGE plpgsql;
