# Ejercicio: ¿Cuánto viven los discos?

Ejercicio de la clase de Big Data (MapReduce, balanceo de carga y Spark Streaming) aplicado al dataset real **Backblaze Drive Stats**, usando una muestra de 10 días consecutivos (2026-04-01 a 2026-04-10, ~3,46M filas).

## Estructura del repo

- `colab/bigdata24oct.ipynb` — Partes A, B y el reto de investigación (Kaplan-Meier). Corre en Google Colab con PySpark.
- `docker-compose.yml`, `Dockerfile.spark`, `.devcontainer/` — entorno local de Kafka + Spark + MongoDB para las Partes C y D.
- `producer/producer.py` — publica los CSV diarios en Kafka simulando llegada en tiempo real (1 día = 10s).
- `streaming/consumer_streaming.py` — Structured Streaming: calcula AFR por modelo en ventanas de 7 días con watermark de 2 días. Modo consola por defecto, modo MongoDB con el argumento `mongo`.
- `data/` — CSV de la muestra (no versionados, ver `.gitignore`; se descargan del ZIP público de Backblaze).

## Parte A: MapReduce

Cálculo de AFR (tasa de falla anualizada) por modelo con RDD (`map` + `reduceByKey`), validado contra `groupBy` de DataFrames. Se identificó el problema de censura: de 347.282 discos observados, solo 194 fallaron en la ventana (347.088 siguen "vivos" sin que se sepa cuándo fallarán).

## Parte B: Balanceo de carga

Medición de tiempos con 2/8/32/200 particiones (entorno de 2 núcleos en Colab): el tiempo apenas cambia entre 2 y 32 particiones, pero se dispara con 200 por el overhead de coordinación. Al repartir por modelo se evidenció sesgo severo: la partición más cargada tuvo 455.875 filas frente a un mínimo de 10 (razón ~45.587x).

## Parte C: Streaming con Kafka

Pipeline completo: `producer.py` publica los 3,46M eventos en el tópico `discos-eventos`; `consumer_streaming.py` los consume con Structured Streaming, agregando AFR por modelo en ventanas de 7 días. Corrido de punta a punta localmente con Docker.

### Cómo levantarlo

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic discos-eventos --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Terminal 1 (consumer, déjalo corriendo)
docker compose exec spark bash -c "cd /workspace && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/consumer_streaming.py"

# Terminal 2 (producer)
docker compose exec spark bash -c "cd /workspace && python producer/producer.py"
```

## Parte D: MongoDB (opcional)

El consumer, corrido con el argumento `mongo`, escribe los agregados (upsert por ventana+modelo) en `discos.agregados_afr`. Consulta de los 5 modelos con mayor AFR (mínimo 5.000 días-disco, para evitar sesgo de muestra pequeña):

```bash
docker compose up -d mongo
docker compose exec spark bash -c "cd /workspace && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/consumer_streaming.py mongo"
```

**Hallazgo**: `HGST HUH721212ALN604` es consistentemente el modelo con peor AFR en la muestra (26,6% y 13,6% en dos ventanas distintas, con volumen suficiente para ser confiable).

## Reto de investigación: Kaplan-Meier (en el notebook de Colab)

Curvas de supervivencia por modelo con `lifelines`. La curva global es casi plana (censura domina con solo 10 días de ventana), pero comparando `HGST HUH721212ALN604` vs. `TOSHIBA MG08ACA16TA` se ve una diferencia clara y estadísticamente significativa (log-rank test, p = 2,33×10⁻²⁶).

## Fuente de datos

[Backblaze Drive Stats](https://www.backblaze.com/drivestats)
