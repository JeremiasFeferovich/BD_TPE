# Proceso de importación

1.  Abrir una nueva sesión de psql, con el comando:

    ``` {frame="single"}
    psql -h host -d database -U user
    ```

    Reemplazando *host, database* y *user* por los valores
    correspondientes.

2.  Correr el código de *funciones.sql* para crear las tablas, la vista,
    el trigger y las funciones.

3.  Correr el siguiente comando para realizar la importación de los
    datos:

    ``` {frame="single"}
    \copy US_BIRTHS_VIEW FROM 
            '/path_to_file/us_births_2016_2021.csv' 
            DELIMITER ',' CSV HEADER;
    ```

4.  Llamar a la función *ReporteConsolidado*:

    ``` {frame="single"}
    SELECT ReporteConsolidado(n);
    ```

    Siendo *n* la cantidad de años que se desean listar.
