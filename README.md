# ETL Visitas Web

Proyecto ETL para procesar datos de visitas web desde servidor SFTP hacia base de datos MySQL, orquestado con Apache Airflow.

## Requisitos

- Python 3.8+
- MySQL
- Apache Airflow
- Acceso a servidor SFTP

## Instalación

### Opción 1: Contenedores (Recomendado)

1. Instalar Docker Desktop

2. Levantar servicios:
```bash
docker-compose up --build
```

3. Acceder:
   - Airflow: http://localhost:8080 (admin/admin)
   - Marquez: http://localhost:3000
   - MySQL: localhost:3306

### Opción 2: Instalación Local

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar base de datos MySQL local

3. Inicializar Airflow:
```bash
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

4. Ejecutar Airflow:
```bash
airflow webserver --port 8080 &
airflow scheduler &
```

## Estructura del Proyecto

- `dags/`: Definición de DAGs de Airflow
- `modules/`: Módulos de extracción, transformación y carga
- `schemas/`: Validación con Pydantic
- `configs/`: Configuraciones
- `integrations/`: Integraciones de monitoreo (OpenLineage)

## Validaciones Implementadas

### Pydantic (Validación a nivel de registro)
- Tipos de datos correctos (string, int, datetime)
- Formatos válidos (email, IP, fechas)
- Valores permitidos (badmail: HARD/empty, baja: SI/empty)
- Rangos numéricos (opens, clicks ≥ 0)

### Great Expectations (Validación a nivel de dataset)
- 15 columnas en orden correcto
- Conteo de filas razonable (1-10000)
- Email no nulo y formato regex
- Valores categóricos permitidos (badmail: HARD/empty)
- Rangos numéricos válidos

### Reglas de Negocio
- Integridad temporal: fecha_open ≥ fecha_envio, fecha_click ≥ fecha_open ∧ fecha_click ≤ fecha_envio
- Deduplicación por email (conserva registro más reciente)
- Normalización de datos (- → NULL, email lowercase)

## Levantar Marquez

Para ejecutar Marquez en Windows:

1. Instalar Docker Desktop
2. Abrir Git Bash o WSL
3. Ejecutar:
```bash
cd marquez
export POSTGRES_PORT=2345
docker compose -f docker-compose.yml -f docker-compose.web.yml -f docker-compose.search.yml up --force-recreate --remove-orphans
```
4. Acceder a http://localhost:3000 para la UI de Marquez

## Notas

- Para desarrollo local, el DAG usa archivos en `data/raw/` en lugar de SFTP
- Configurado para MySQL local con usuario root sin password
- Omitiendo Sentry y Slack por ahora, solo OpenLineage para linaje
- Para ejecutar manualmente: `python -c "from dags.etl_visitas import extract_task, transform_task, load_task; files=extract_task(); data=transform_task(files); load_task(data, files)"`
- Backup automático: Archivos procesados se comprimen en zip y eliminan tras carga exitosa
- Métricas/KPIs: Se recopilan y reportan archivos procesados, registros válidos vs errores, tiempos de ejecución por etapa, alertas
- Para probar transformación: `python tests/test_etl.py`
