# ETL con Apache Airflow y MySQL

Este proyecto implementa un pipeline ETL utilizando **Apache Airflow**, **MySQL** y **Docker**. El objetivo es:

1. Extraer datos desde una API pública.
2. Guardarlos como archivo `.csv`.
3. Cargarlos a una base de datos MySQL.
4. Generar un reporte de agregación también en `.csv`.