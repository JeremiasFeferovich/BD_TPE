/*
    Deben crearse las tablas de las distintas dimensiones con las siguientes condiciones mínimas:
• ESTADO: debe tener un campo identificatorio y un campo con el nombre del estado
• ANIO: debe tener un campo identificatorio y un campo que indique si el año es bisiesto
• NIVEL_EDUCACION: debe tener un campo identificatorio y un campo con la descripción del
nivel de educación
Se deben crear las claves y constraints apropiados.
*/

CREATE TABLE STATE (
    NAME VARCHAR(50) NOT NULL,
    ABBR VARCHAR(2) NOT NULL,
    PRIMARY KEY (ABBR),
    UNIQUE (NAME)
);

CREATE TABLE YEAR_DATA (
    YEAR_NUMBER INT NOT NULL,
    LEAP_YEAR BOOLEAN NOT NULL,
    PRIMARY KEY (YEAR_NUMBER)
);

CREATE TABLE EDUCATION_LEVEL (
    EDUCATION_LEVEL_CODE INT NOT NULL,
    EDUCATION_LEVEL VARCHAR(50) NOT NULL,
    PRIMARY KEY (EDUCATION_LEVEL_CODE)
);

/*
 Creación de la tabla definitiva.
Debe crearse una tabla definitiva que será la receptora de los datos provenientes del archivo
us_births_2016_2021.csv. Los campos y restricciones de la tabla deben crearse en base al análisis
de los datos.
Recordar que los archivos csv son archivos de texto que pueden abrirse fácilmente con cualquier
editor.
Para el caso particular de los campos state, state_abbreviation, year, mother_education_level y
education_level_code, se deberá cambiar su contenido para que el mismo haga referencia a la key
de la tablas creadas en el punto a), antes de insertarlos en la tabla definitiva.
En base a los datos, se debe crear la clave y constraints apropiados.

Las columnas del archivo son:
● State: nombre del estado de Estados Unidos del cual provienen los nacimientos
● State_Abbreviation: código del nombre del estado de Estados Unidos
● Year: año calendario en el cual se sucedieron los nacimientos
● Gender: código del género de los bebés
● Mother_Education_Level: nivel de educación de las madres de los bebés
● Education_Level_Code: código del nivel de educación de las madres de los bebés
● Births: cantidad total de bebés nacidos
● Mother_Average_Age: promedio de edad de las madres de los bebés, expresado en años
● Average_Birth_Weight: promedio del peso de los bebés, expresado en gramos
Antes de insertar el archivo en una tabla definitiva, se quiere interceptar la inserción del estado, del
año y del nivel de educación de las madres, y cambiarlas por una FK a diferentes dimensiones
representadas en las tablas ESTADO, ANIO y NIVEL_EDUCACION.
*/

CREATE TABLE US_BIRTHS (
    STATE VARCHAR(2) NOT NULL,
    YEAR_NUMBER INT NOT NULL,
    EDUCATION_LEVEL INT NOT NULL,
    GENDER VARCHAR(1) NOT NULL,
    BIRTHS INT NOT NULL,
    MOTHER_AVG_AGE INT NOT NULL,
    AVG_BIRTH_WEIGHT INT NOT NULL,
    FOREIGN KEY (STATE) REFERENCES STATE(ABBR),
    FOREIGN KEY (YEAR_NUMBER) REFERENCES YEAR_DATA(YEAR_NUMBER),
    FOREIGN KEY (EDUCATION_LEVEL) REFERENCES EDUCATION_LEVEL(EDUCATION_LEVEL_CODE),
    PRIMARY KEY (STATE,YEAR_NUMBER,EDUCATION_LEVEL,GENDER)
);


/*
c) Importación de los datos.
Utilizando el comando COPY de PostgreSQL, se deben importar TODOS los datos del archivo csv en
la tabla creada en b). El archivo csv provisto por la cátedra NO puede ser modificado.
*/

COPY US_BIRTHS FROM '/us_births_2016_2021.csv' DELIMITER ',' CSV HEADER;

/*
Creación de un trigger para:
1) Determinar la FK de las distintas dimensiones
Para insertar los datos en la tabla definitiva es necesario interceptar la inserción del estado, año y
nivel educativo de la madre, y luego identicar la FK a cada una de las dimensiones de las tablas
creadas en el punto a).
2) Cargar los valores de las dimensiones
Además de insertar los datos del archivo, se deben poblar las distintas tablas que conforman las
distintas dimensiones, siempre y cuando los valores correspondientes no existan en dichas tablas.

Por ejemplo, si partimos con la tabla ESTADO vacía y si al principio en el archivo CSV viene el estado
”Alabama” con la abreviación ”AL”, se debe insertar una tupla en la tabla ESTADO, quedando la tabla
con la siguiente información:
• ESTADO: ”AL”, ”Alabama”

*/

CREATE OR REPLACE FUNCTION is_leap_year(year_number INT) RETURNS BOOLEAN AS $$
    BEGIN
        RETURN year_number % 4 = 0 || (year_number % 100 = 0 && year_number % 400 = 0);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_us_births() RETURNS TRIGGER AS $$
    DECLARE
        state_abbr INT;
        year_number INT;
        education_level_code INT;
        is_leap_year BOOLEAN;
        data_to_insert US_BIRTHS%ROWTYPE;
    BEGIN
        SELECT ABBR INTO state_abbr FROM STATE WHERE ABBR = NEW.State_Abbreviation;
        IF NOT FOUND THEN
            INSERT INTO STATE (ABBR, NAME) VALUES (NEW.State_Abbreviation, NEW.State);
        END IF;

        SELECT YEAR_NUMBER INTO year_number FROM YEAR_DATA WHERE YEAR_NUMBER = NEW.Year;
        IF NOT FOUND THEN
            is_leap_year = is_leap_year(NEW.Year);
            INSERT INTO YEAR_DATA (YEAR_NUMBER, LEAP_YEAR) VALUES (NEW.YEAR_NUMBER, is_leap_year);
        END IF;

        SELECT EDUCATION_LEVEL_CODE INTO education_level_code FROM EDUCATION_LEVEL WHERE EDUCATION_LEVEL_CODE = NEW.Education_Level_Code;
        IF NOT FOUND THEN
            INSERT INTO EDUCATION_LEVEL (EDUCATION_LEVEL_CODE, EDUCATION_LEVEL) VALUES (NEW.Education_Level_Code, NEW.Mother_Education_Level);
        END IF;

        data_to_insert.STATE = state_abbr;
        data_to_insert.YEAR_NUMBER = year_number;
        data_to_insert.EDUCATION_LEVEL = education_level_code;
        data_to_insert.gender = NEW.gender;
        data_to_insert.births = NEW.births;
        data_to_insert.mother_avg_age = NEW.mother_average_age;
        data_to_insert.avg_birth_weight = NEW.average_birth_weight;

        RETURN data_to_insert;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_us_births_trigger BEFORE INSERT ON US_BIRTHS FOR EACH ROW EXECUTE PROCEDURE insert_us_births();





/*
d) Reporte de información consolidada.
Se pide crear la función ReporteConsolidado(n) que recibe como parámetro la cantidad de años a
mostrar tomando como base el primer año cargado en la tabla definitiva, la cual genere un reporte
mostrando para cada año y categoría, la cantidad total de nacimientos, la edad promedio de las
madres, la edad mínima, la edad máxima, el promedio de peso de los bebés, el peso mínimo y el
peso máximo. Los 3 pesos expresados en kilogramos.
El reporte tendrá las siguientes características:
I. Título del reporte:
 "CONSOLIDATED BIRTH REPORT”
II. Encabezado de columnas:
“Year Category Total AvgAge MinAge MaxAge AvgWeight MinWeight
MaxWeight”
III. Por cada año tiene que aparecer un renglón en el reporte, con los años ordenados de menor
a mayor. La primer categoría de agrupación (State) con sus valores ordenados
alfabéticamente en forma descendente y sus métricas (Total, AvgAge, MinAge, MaxAge,
AvgWeight, MinWeight y MaxWeight), deben estar en el mismo renglón que el año. El resto
de las categorías (Gender y Education Level), encolumnados a continuación en los renglones
subsiguientes:
o Para la categoría de Estados, solo interesa reportar aquellos donde haya habido más de
200.000 nacimientos
o Para la categoría de Nivel de Educación de la madre, solo interesa reportar los niveles de
educación categorizados con algún valor relevante. Es decir, no considerar cuando el nivel de
educación es desconocido o no informado
IV. Al final de los renglones, tiene que aparecer el total de las métricas Total, AvgAge, MinAge,
MaxAge, AvgWeight, MinWeight y MaxWeight correspondientes para ese año
En caso de que no existieran datos para los parámetros ingresados, no se debe mostrar nada (ni
siquiera el encabezado del reporte).
*/

CREATE OR REPLACE FUNCTION ReporteConsolidado(n INT) RETURNS VOID AS $$
DECLARE
    first_year INTEGER;
    last_year INTEGER;
    current_year INTEGER;
    state_code TEXT;
    gender_code TEXT;
    education_level_code TEXT;
    total_births BIGINT;
    avg_age NUMERIC;
    min_age INTEGER;
    max_age INTEGER;
    avg_weight NUMERIC;
    min_weight NUMERIC;
    max_weight NUMERIC;
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
    RAISE NOTICE 'CONSOLIDATED BIRTH REPORT';
    RAISE NOTICE 'Year Category Total AvgAge MinAge MaxAge AvgWeight MinWeight MaxWeight';

    -- Iterate over each year
    FOR current_year IN first_year..last_year LOOP
        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT
            SUM(TOTAL_BIRTHS),
            AVG(MOTHER_AGE),
            MIN(MOTHER_AGE),
            MAX(MOTHER_AGE),
            AVG(BABY_WEIGHT),
            MIN(BABY_WEIGHT),
            MAX(BABY_WEIGHT)
        INTO
            total_births,
            avg_age,
            min_age,
            max_age,
            avg_weight,
            min_weight,
            max_weight
        FROM
            US_BIRTHS
        WHERE
            YEAR_NUMBER = current_year;

        -- Check if there is data for the year
        IF total_births IS NOT NULL THEN
            -- Print the year, state category, and corresponding metrics
            RAISE NOTICE '% % % % % % % % % % % %',
                current_year,
                'State',
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight;

            -- Iterate over each state with more than 200,000 births
            FOR state_code IN (
                SELECT
                    STATE.ABBR
                FROM
                    US_BIRTHS
                    JOIN STATE ON US_BIRTHS.STATE = STATE.ID
                WHERE
                    YEAR_NUMBER = current_year
                GROUP BY
                    STATE.ABBR
                HAVING
                    SUM(TOTAL_BIRTHS) > 200000
                ORDER BY
                    STATE.ABBR DESC
            ) LOOP
                -- Get the metrics for the current state and year
                SELECT
                    SUM(TOTAL_BIRTHS),
                    AVG(MOTHER_AGE),
                    MIN(MOTHER_AGE),
                    MAX(MOTHER_AGE),
                    AVG(BABY_WEIGHT),
                    MIN(BABY_WEIGHT),
                    MAX(BABY_WEIGHT)
                INTO
                    total_births,
                    avg_age,
                    min_age,
                    max_age,
                    avg_weight,
                    min_weight,
                    max_weight
                FROM
                    US_BIRTHS
                    JOIN STATE ON US_BIRTHS.STATE = STATE.ID
                WHERE
                    YEAR_NUMBER = current_year
                    AND STATE.ABBR = state_code;

                -- Print the state and corresponding metrics
                RAISE NOTICE '% % % % % % % % % % % %',
                    '',
                    state_code,
                    total_births,
                    avg_age,
                    min_age,
                    max_age,
                    avg_weight,        
                    min_weight,
                    max_weight;
                    END LOOP;
                    

        -- Iterate over each gender category
        FOR gender_code IN (
            SELECT DISTINCT GENDER FROM US_BIRTHS
            WHERE YEAR_NUMBER = current_year
        ) LOOP
            -- Get the metrics for the current gender and year
            SELECT
                SUM(TOTAL_BIRTHS),
                AVG(MOTHER_AGE),
                MIN(MOTHER_AGE),
                MAX(MOTHER_AGE),
                AVG(BABY_WEIGHT),
                MIN(BABY_WEIGHT),
                MAX(BABY_WEIGHT)
            INTO
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight
            FROM
                US_BIRTHS
            WHERE
                YEAR_NUMBER = current_year
                AND GENDER = gender_code;

            -- Print the gender and corresponding metrics
            RAISE NOTICE '% % % % % % % % % % % %',
                '',
                gender_code,
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight;
        END LOOP;

        -- Iterate over each education level category
        FOR education_level_code IN (
            SELECT DISTINCT EDUCATION_LEVEL_CODE FROM US_BIRTHS
            WHERE YEAR_NUMBER = current_year
                AND EDUCATION_LEVEL_CODE NOT IN ('Unknown', 'Not reported')
        ) LOOP
            -- Get the metrics for the current education level and year
            SELECT
                SUM(TOTAL_BIRTHS),
                AVG(MOTHER_AGE),
                MIN(MOTHER_AGE),
                MAX(MOTHER_AGE),
                AVG(BABY_WEIGHT),
                MIN(BABY_WEIGHT),
                MAX(BABY_WEIGHT)
            INTO
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight
            FROM
                US_BIRTHS
            WHERE
                YEAR_NUMBER = current_year
                AND EDUCATION_LEVEL_CODE = education_level_code;

            -- Print the education level and corresponding metrics
            RAISE NOTICE '% % % % % % % % % % % %',
                '',
                education_level_code,
                total_births,
                avg_age,
                min_age,
                max_age,
                avg_weight,
                min_weight,
                max_weight;
        END LOOP;

        -- Print the total metrics for the year
        RAISE NOTICE '% Total % % % % % % % %',
            '',
            total_births,
            avg_age,
            min_age,
            max_age,
            avg_weight,
            min_weight,
            max_weight;
    END IF;
END LOOP;
