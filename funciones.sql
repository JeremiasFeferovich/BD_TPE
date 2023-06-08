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

