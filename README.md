# Gestión de Datos de Turbina de Viento
## Autor: Imanol Anda Garcia
Este proyecto aborda la implementación de una solución de base de datos para la gestión eficiente de los datos generados por una turbina de viento. Emplea InfluxDB para almacenar y procesar datos de series temporales, lo que permite realizar análisis complejos y consultas de agregación.

## Descripción del Proyecto
- Se maneja un dataset de una turbina de viento, que incluye potencia activa, velocidad del viento, curva teórica de potencia y dirección del viento.
- Se utiliza InfluxDB como sistema de gestión de base de datos, optimizado para el manejo de datos de series temporales.
- La aplicación desarrollada en Python lee y transfiere los datos del dataset a InfluxDB, manteniendo la integridad y la precisión de las marcas de tiempo.
- Se realizan agregaciones de datos, como la media de potencia activa y la velocidad promedio del viento, demostrando la capacidad de análisis de la base de datos.
## Instrucciones de Uso
### Configurar y Ejecutar la Aplicación de Carga de Datos
- Asegúrate de tener Python 3.8 o superior y InfluxDB instalados en tu entorno.
- Configura tu cliente InfluxDB con los parámetros correctos (URL, token, organización y bucket).
- Ejecuta python load_data.py para transferir datos desde el dataset T1.csv a InfluxDB.
## Consultas en InfluxDB
Utiliza el cliente de InfluxDB o la interfaz de línea de comandos para realizar consultas de agregación.
Ejemplo de consulta: SELECT MEAN("Wind Speed (m/s)") FROM "turbina" para obtener la velocidad media del viento.
## Argumentación de la selección
- Optimizada para Series Temporales: InfluxDB es una base de datos específicamente diseñada para manejar series temporales, lo cual es ideal para datos de sensores o lecturas que se registran en intervalos de tiempo regulares, como es el caso de las mediciones de una turbina de viento.
- Alto Rendimiento en Lecturas y Escrituras: InfluxDB maneja eficientemente altas tasas de escritura y consulta, lo que resulta fundamental para entornos donde los datos son recopilados en tiempo real y necesitan ser procesados rápidamente.
- Consultas Eficientes de Datos Temporales: InfluxDB utiliza un lenguaje de consulta (InfluxQL o Flux) optimizado para operaciones de tiempo, permitiendo realizar consultas complejas de forma más eficiente que las bases de datos tradicionales.
- Soporte para Datos de Alta Precisión: El soporte para marcas de tiempo con precisión de nanosegundos es particularmente útil para conjuntos de datos que requieren alta resolución temporal.
## Alternativas posibles 
### TimescaleDB
TimescaleDB es una extensión de PostgreSQL diseñada específicamente para datos de series temporales. Hereda la fiabilidad y el rico ecosistema de herramientas de PostgreSQL, añadiendo optimizaciones para manejar grandes volúmenes de datos temporales.

- Compatibilidad con SQL: Si ya estás familiarizado con SQL, TimescaleDB ofrece una curva de aprendizaje suave.
- Escalabilidad: Proporciona funciones automáticas de particionamiento que facilitan la escalabilidad horizontal.
- Integración con PostgreSQL: Al ser una extensión, se beneficia de las características de seguridad y las capacidades de PostgreSQL, así como de su extenso conjunto de herramientas y extensiones.
### Prometheus
Prometheus es un sistema de monitorización y alertas de series temporales diseñado principalmente para la monitorización de sistemas y aplicaciones.

- Modelo de Datos Multidimensional: Las métricas de series temporales en Prometheus se identifican con un nombre y un conjunto de pares clave-valor, lo que es útil para el seguimiento y la consulta de métricas específicas de turbinas.
- Operaciones de Agregación en Tiempo Real: Soporta operaciones de agregación complejas, lo cual es ideal para la generación de alertas y la visualización de datos en tiempo real.
- Alto Rendimiento: Aunque no es una base de datos en el sentido tradicional, Prometheus está optimizado para almacenar series temporales con alta eficiencia y rendimiento.
