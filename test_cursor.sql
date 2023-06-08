
CREATE TYPE us_births_view_type AS (
    births BIGINT,
    avg_mother_average_age INTEGER,
    average_birth_weight DECIMAL(4, 3),
);


CREATE OR REPLACE FUNCTION PrintMetrics(pData us_births_view_type, pCategory TEXT) RETURNS VOID AS
$$
DECLARE
    total_births BIGINT;
    avg_age      INTEGER;
    min_age      INTEGER;
    max_age      INTEGER;
    avg_weight   DECIMAL(4, 3);
    min_weight   DECIMAL(4, 3);
    max_weight   DECIMAL(4, 3);
BEGIN
    SELECT SUM(rowData.births),
           CAST(ROUND(AVG(rowData.mother_average_age)) AS INTEGER),
           CAST(ROUND(MIN(rowData.mother_average_age)) AS INTEGER),
           CAST(ROUND(MAX(rowData.mother_average_age)) AS INTEGER),
           CAST(AVG(rowData.average_birth_weight) / 1000 AS DECIMAL(4, 3)),
           CAST(MIN(rowData.average_birth_weight) / 1000 AS DECIMAL(4, 3)),
           CAST(MAX(rowData.average_birth_weight) / 1000 AS DECIMAL(4, 3))
    INTO
        total_births,
        avg_age,
        min_age,
        max_age,
        avg_weight,
        min_weight,
        max_weight
    FROM (SELECT pData.births, pData.mother_average_age, pData.average_birth_weight) AS rowData;

    -- Print the state and corresponding metrics
    RAISE INFO '% % % % % % % %',
        pCategory,
        FORMAT('%-10s', total_births),
        FORMAT('%-10s', avg_age),
        FORMAT('%-10s', min_age),
        FORMAT('%-10s', max_age),
        FORMAT('%-10s', avg_weight),
        FORMAT('%-10s', min_weight),
        FORMAT('%-10s', max_weight);
END
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION ReportStates(pYear INT) RETURNS VOID AS
$$
DECLARE
    rState  US_BIRTHS_VIEW.STATE%TYPE;
    cState CURSOR FOR
        SELECT STATE
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
        GROUP BY STATE
        HAVING SUM(BIRTHS) > 200000
        ORDER BY STATE DESC;
    pData   us_births_view_type;
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
        WHERE STATE = rState
          AND YEAR = pYear;

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
    rGender TEXT;
    cGender CURSOR FOR
        SELECT GENDER
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
        GROUP BY GENDER
        ORDER BY GENDER DESC;
    pData   us_births_view_type;
BEGIN
    OPEN cGender;
    LOOP
        FETCH NEXT FROM cGender INTO rGender;
        EXIT WHEN NOT FOUND;

        SELECT state, year, births, mother_average_age, average_birth_weight
        INTO pData
        FROM US_BIRTHS_VIEW
        WHERE GENDER = rGender
          AND YEAR = pYear;

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
    rEducation_level INTEGER;
    cEducation_level CURSOR FOR
        SELECT EDUCATION_LEVEL_CODE
        FROM US_BIRTHS_VIEW
        WHERE YEAR = pYear
          AND EDUCATION_LEVEL_CODE <> -9
        GROUP BY EDUCATION_LEVEL_CODE;
    pData            us_births_view_type;
BEGIN
    OPEN cEducation_level;
    LOOP
        FETCH NEXT FROM cEducation_level INTO rEducation_level;
        EXIT WHEN NOT FOUND;

        -- Get the metrics for the current education level and year
        SELECT state, year, births, mother_average_age, average_birth_weight
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
    first_year   INTEGER;
    last_year    INTEGER;
    total_births BIGINT;
    avg_age      INTEGER;
    min_age      INTEGER;
    max_age      INTEGER;
    avg_weight   DECIMAL(4, 3);
    min_weight   DECIMAL(4, 3);
    max_weight   DECIMAL(4, 3);
    cYear CURSOR FOR
        SELECT YEAR
        FROM YEAR_DATA
        WHERE YEAR BETWEEN first_year AND last_year;
    pYear        INTEGER;
    pData        us_births_view_type;

BEGIN
    -- Get the first year in the table
    SELECT MIN(YEAR) INTO first_year FROM YEAR_DATA;

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


    OPEN cYear;
    LOOP
        FETCH NEXT FROM cYear INTO pYear;
        EXIT WHEN NOT FOUND;

        -- Iterate over each state with more than 200,000 births
        PERFORM ReportStates(pYear);

        -- Iterate over each gender category
        PERFORM ReportGender(pYear);

        -- Iterate over each education level category
        PERFORM ReportEducationLevel(pYear);

        -- SELECT state, year, births, mother_average_age, average_birth_weight INTO pData FROM US_BIRTHS_VIEW WHERE education_level_code = rEducation_level AND YEAR = pYear;

        --  PERFORM PrintMetrics(pData, FORMAT('---- %-80s', FORMAT('Education: %s', (SELECT mother_education_level FROM EDUCATION_LEVEL WHERE education_level_code = rEducation_level))));

        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT SUM(BIRTHS),
               CAST(ROUND(AVG(mother_average_age)) AS INTEGER),
               CAST(ROUND(MIN(mother_average_age)) AS INTEGER),
               CAST(ROUND(MAX(mother_average_age)) AS INTEGER),
               CAST(AVG(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MIN(average_birth_weight) / 1000 AS DECIMAL(4, 3)),
               CAST(MAX(average_birth_weight) / 1000 AS DECIMAL(4, 3))
        INTO
            total_births,
            avg_age,
            min_age,
            max_age,
            avg_weight,
            min_weight,
            max_weight
        FROM US_BIRTHS
        WHERE YEAR = pYear;

        -- Print the total metrics for the year
        RAISE INFO '---- % % % % % % % %',
            FORMAT('%-80s', ''),
            FORMAT('%-10s', total_births),
            FORMAT('%-10s', avg_age),
            FORMAT('%-10s', min_age),
            FORMAT('%-10s', max_age),
            FORMAT('%-10s', avg_weight),
            FORMAT('%-10s', min_weight),
            FORMAT('%-10s', max_weight);

        RAISE INFO '---------------------------------------------------------------------------------------------------------';
    END LOOP;
    CLOSE cYear;

    RAISE INFO '---------------------------------------------------------------------------------------------------------';
END;
$$ LANGUAGE plpgsql;
