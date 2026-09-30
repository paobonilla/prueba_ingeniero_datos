# Prueba Técnica — Ingeniero de datos Jr

Solución a la prueba técnica de ingeniería de datos desarrollada en PySpark y Delta Lake.

## Estructura del Proyecto

- `Ejercicio1.py`: Transformación de datos semiestructurados (MongoDB/Bronze) a esquema Silver, normalización de arrays de compras a `customer_purchases_silver`, manejo explícito de nulos y particionamiento por país y fecha.
- `Ejercicio2.py`: Pipeline de merge incremental para órdenes en capa Silver con deduplicación previa de clientes mediante funciones de ventana, uso de llaves compuestas (`order_id_txt`, `pais_cd`) y particionamiento multi-país.
- `Ejercicio3.py`: Carga incremental (`incremental_load`) con Delta Lake, parametrizada mediante diccionario de configuración y protegida por watermark temporal (`updated_ts`) contra eventos fuera de orden o desfasados.
- `Respuestas_Ejercicios2.txt`: Justificaciones técnicas y respuestas a las preguntas teóricas de los ejercicios 2.
- `Respuestas_Ejercicios3.txt`: Justificaciones técnicas y respuestas a las preguntas teóricas de los ejercicios 3.

## Requisitos y Configuración

- Python 3.10+
- Java (JDK 8, 11 o 17)
- Apache Spark 3.5+ con Delta Lake 3.x+

### Instalación de Dependencias

```bash
pip install -r requirements.txt
