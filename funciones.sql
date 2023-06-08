/*
    Deben crearse las tablas de las distintas dimensiones con las siguientes condiciones mínimas:
• ESTADO: debe tener un campo identificatorio y un campo con el nombre del estado
• ANIO: debe tener un campo identificatorio y un campo que indique si el año es bisiesto
• NIVEL_EDUCACION: debe tener un campo identificatorio y un campo con la descripción del
nivel de educación
Se deben crear las claves y constraints apropiados.
*/

CREATE TABLE STATE (
    STATE VARCHAR(50) NOT NULL,
    STATE_ABBREVIATION VARCHAR(2) NOT NULL,
    PRIMARY KEY (STATE_ABBREVIATION),
    UNIQUE (STATE)
);

CREATE TABLE YEAR_DATA (
    YEAR INT NOT NULL,
    LEAP BOOLEAN NOT NULL,
    PRIMARY KEY (YEAR)
);

CREATE TABLE EDUCATION_LEVEL (
    EDUCATION_LEVEL_CODE INT NOT NULL,
    MOTHER_EDUCATION_LEVEL VARCHAR(255) NOT NULL,
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
    STATE_ABBREVIATION VARCHAR(2) NOT NULL,
    YEAR INT NOT NULL,
    EDUCATION_LEVEL_CODE INT NOT NULL,
    GENDER VARCHAR(1) NOT NULL,
    BIRTHS INT NOT NULL,
    MOTHER_AVERAGE_AGE DECIMAL(3,1) NOT NULL,
    AVERAGE_BIRTH_WEIGHT DECIMAL(5,1) NOT NULL,
    FOREIGN KEY (STATE_ABBREVIATION) REFERENCES STATE(STATE_ABBREVIATION),
    FOREIGN KEY (YEAR) REFERENCES YEAR_DATA(YEAR),
    FOREIGN KEY (EDUCATION_LEVEL_CODE) REFERENCES EDUCATION_LEVEL(EDUCATION_LEVEL_CODE),
    PRIMARY KEY (STATE_ABBREVIATION,YEAR,EDUCATION_LEVEL_CODE,GENDER)
);


/*
c) Importación de los datos.
Utilizando el comando COPY de PostgreSQL, se deben importar TODOS los datos del archivo csv en
la tabla creada en b). El archivo csv provisto por la cátedra NO puede ser modificado.
*/

--\copy US_BIRTHS FROM '/home/jerefefe/Desktop/BD/BD_TPE/us_births_2016_2021.csv' DELIMITER ',' CSV HEADER;

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

CREATE VIEW US_BIRTHS_VIEW AS 
        SELECT STATE.STATE AS STATE, 
                STATE.STATE_ABBREVIATION AS STATE_ABBREVIATION, 
                YEAR_DATA.YEAR AS YEAR, 
                GENDER, 
                EDUCATION_LEVEL.MOTHER_EDUCATION_LEVEL AS MOTHER_EDUCATION_LEVEL,
                EDUCATION_LEVEL.EDUCATION_LEVEL_CODE AS EDUCATION_LEVEL_CODE,
                BIRTHS, 
                MOTHER_AVERAGE_AGE, 
                AVERAGE_BIRTH_WEIGHT
                
        FROM 
                US_BIRTHS JOIN STATE ON US_BIRTHS.STATE_ABBREVIATION = STATE.STATE_ABBREVIATION
                JOIN YEAR_DATA ON US_BIRTHS.YEAR = YEAR_DATA.YEAR
                JOIN EDUCATION_LEVEL ON US_BIRTHS.EDUCATION_LEVEL_CODE = EDUCATION_LEVEL.EDUCATION_LEVEL_CODE;



CREATE OR REPLACE FUNCTION is_leap_year(year_number INT) RETURNS BOOLEAN AS $$
    BEGIN
        RETURN year_number % 4 = 0 OR (year_number % 100 = 0 AND year_number % 400 = 0);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_us_births() RETURNS TRIGGER AS $$
    DECLARE
        state_id VARCHAR(3);
        year_id INT;
        education_level_id INT;
        is_leap_year BOOLEAN;
        -- data_to_insert US_BIRTHS%ROWTYPE;
    BEGIN
        SELECT STATE_ABBREVIATION INTO state_id FROM STATE WHERE STATE_ABBREVIATION = NEW.State_Abbreviation;
        IF NOT FOUND THEN
            INSERT INTO STATE (STATE_ABBREVIATION, STATE) VALUES (NEW.State_Abbreviation, NEW.State);
        END IF;

        SELECT YEAR INTO year_id FROM YEAR_DATA WHERE YEAR = NEW.Year;
        IF NOT FOUND THEN
            is_leap_year = is_leap_year(NEW.Year);
            INSERT INTO YEAR_DATA (YEAR, LEAP) VALUES (NEW.YEAR, is_leap_year);
        END IF;

        SELECT EDUCATION_LEVEL_CODE INTO education_level_id FROM EDUCATION_LEVEL WHERE EDUCATION_LEVEL_CODE = NEW.Education_Level_Code;
        IF NOT FOUND THEN
            INSERT INTO EDUCATION_LEVEL (EDUCATION_LEVEL_CODE, MOTHER_EDUCATION_LEVEL) VALUES (NEW.Education_Level_Code, NEW.Mother_Education_Level);
        END IF;
                    
        INSERT INTO US_BIRTHS VALUES (NEW.State_Abbreviation, NEW.Year, NEW.Education_Level_Code, NEW.gender, NEW.births, NEW.mother_average_age, NEW.average_birth_weight );
        RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER insert_us_births_trigger INSTEAD OF INSERT ON US_BIRTHS_VIEW FOR EACH ROW EXECUTE PROCEDURE insert_us_births();





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
    education_level_code INTEGER;
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
    RAISE NOTICE 'CONSOLIDATED BIRTH REPORT';
    RAISE NOTICE 'Year Category Total AvgAge MinAge MaxAge AvgWeight MinWeight MaxWeight';

    -- Iterate over each year
    FOR current_year IN first_year..last_year LOOP
        -- Get the total births, average age, minimum age, maximum age, average weight, minimum weight, and maximum weight for the year
        SELECT
            SUM(BIRTHS),
            CAST(AVG(MOTHER_AVG_AGE) AS INTEGER),
            CAST(MIN(MOTHER_AVG_AGE) AS INTEGER),
            CAST(MAX(MOTHER_AVG_AGE) AS INTEGER),
            CAST(AVG(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
            CAST(MIN(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
            CAST(MAX(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3))
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
            RAISE NOTICE '% % % % % % % % %',
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
                    STATE
                FROM
                    US_BIRTHS_VIEW
                WHERE
                    YEAR = current_year
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
                        CAST(AVG(MOTHER_AVG_AGE) AS INTEGER),
                        CAST(MIN(MOTHER_AVG_AGE) AS INTEGER),
                        CAST(MAX(MOTHER_AVG_AGE) AS INTEGER),
                        CAST(AVG(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                        CAST(MIN(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                        CAST(MAX(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3))
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
                    JOIN STATE ON US_BIRTHS.STATE = STATE.ABBR
                WHERE
                    YEAR_NUMBER = current_year
                    AND STATE.ABBR = state_code;

                -- Print the state and corresponding metrics
                RAISE NOTICE '% % % % % % % % %',
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
                SUM(BIRTHS),
                CAST(AVG(MOTHER_AVG_AGE) AS INTEGER),
                CAST(MIN(MOTHER_AVG_AGE) AS INTEGER),
                CAST(MAX(MOTHER_AVG_AGE) AS INTEGER),
                CAST(AVG(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                CAST(MIN(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                CAST(MAX(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3))
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
            RAISE NOTICE '% % % % % % % % %',
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
            SELECT DISTINCT EDUCATION_LEVEL FROM US_BIRTHS
            WHERE YEAR_NUMBER = current_year
                AND EDUCATION_LEVEL > 0
        ) LOOP
            -- Get the metrics for the current education level and year
            SELECT
                SUM(BIRTHS),
                CAST(AVG(MOTHER_AVG_AGE) AS INTEGER),
                CAST(MIN(MOTHER_AVG_AGE) AS INTEGER),
                CAST(MAX(MOTHER_AVG_AGE) AS INTEGER),
                CAST(AVG(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                CAST(MIN(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3)),
                CAST(MAX(AVG_BIRTH_WEIGHT) AS DECIMAL(4,3))
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
                AND EDUCATION_LEVEL = education_level_code;

            -- Print the education level and corresponding metrics
            RAISE NOTICE '% % % % % % % % %',
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
        RAISE NOTICE '% Total % % % % % % %',
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
    END;
$$ LANGUAGE plpgsql;

SELECT ReporteConsolidado(1) FROM education_level

