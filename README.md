# Replicación de Data Warehouse (Bodega de Datos)
 
## Herramientas utilizadas
- MySql (Base de Datos)
- Visual Studio Code (IDE)
- Python (Lenguaje de programación)

## Requisitos para la implementación
* Instalar MySql Workbenck 8.0
* Instalar Python en nuestra máquina
* Crear las bases de datos ESSGDBSOR y ESSGDBSTG y replicar los "MODELOS DE DATOS" de la misma manera que las imagenes en la carpeta `evidencesSem3`
* Clonar el repositorio y abrir el archivo en el IDE Visual Studio Code

## Instalar las librerias necesarias
```bash
pip install PyMySQL
pip install configparser
pip install 
```

## Configuración del archivo .properties
En este archivo tendremos que cambiar las credenciales por las de nuestra base de datos 
- Ejemplo:
```bash
[DatabaseCredentials]
DB_TYPE = mysql
DB_HOST = localhost
DB_PORT = 3306
DB_USER = esteban_udla
DB_PWD = estabn_udla
STG_NAME = essgdbstg
SOR_NAME = essgdbsor
```

## Ejecución del proyecto
Vamos a dirigirnos a la terminal de Visual Studio Code y vamos a ingresar el siguiente comando:
```bash
python .\py_startup.py
```
## Validar la información en MySql
Al entrar a nuestro DBMS tendremos que verificar que las tablas y los datos en la base de datos `STAGING` han sido creadas correctamente.

## Transform & Load
Los avances de la Semana 4 con sus respectivas pruebas se encuentran en la carpeta `EvidencesSem4`
Para obtener la última versión del proyecto se deberá ejecutar el archivo `latest_startup.py` en la terminal correspondiente


#### Desarrollado por: Esteban Sampedro



