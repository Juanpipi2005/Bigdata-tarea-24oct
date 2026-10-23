# Streaming (Partes C y D): Kafka + Spark Structured Streaming

## Estructura
- `docker-compose.yml`, `Dockerfile.spark`: Kafka (KRaft) y contenedor Spark/PySpark en la misma red.
- `producer/producer.py`: publica los CSV de `data/` en el tópico `discos-eventos`.
- `streaming/consumer_streaming.py`: calcula AFR por modelo en ventanas de 7 días.

Copia los 10 CSV (`2026-04-01.csv` … `2026-04-10.csv`) en `data/`; se ordenan por nombre.

## Pasos
Todo desde la raíz del repo. Puedes abrir VS Code con "Reopen in Container" o usar `docker compose exec spark ...`.

1. Levantar servicios:
   ```
   docker compose up -d --build
   ```
2. Crear el tópico:
   ```
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
     --create --topic discos-eventos --partitions 1 --replication-factor 1
   ```
3. Terminal A: consumer primero (queda esperando datos):
   ```
   docker compose exec spark spark-submit --packages $SPARK_KAFKA_PACKAGE streaming/consumer_streaming.py
   ```
   (La primera vez descarga los jars; queda cacheado en un volumen.)
4. Terminal B: producer (10 s entre días, ~90 s en total):
   ```
   docker compose exec spark python producer/producer.py
   ```
   Si un día supera ~350 mil filas se muestrea solo; o fuerza filtro/muestreo:
   ```
   python producer/producer.py --models "MODELO1,MODELO2,MODELO3"
   python producer/producer.py --sample 0.2
   ```
5. Resultados: la terminal A imprime tablas por micro-batch (`ventana_inicio, ventana_fin, model, dias_disco, fallas, afr`). Para quitar el ruido de logs ya está en nivel WARN.

Desde el host (sin contenedor) el producer usa `--bootstrap localhost:29092`.

## Parte D (MongoDB, opcional)
Descomenta el servicio `mongo` en `docker-compose.yml`, `docker compose up -d`, y corre el consumer con el argumento `mongo`:
```
docker compose exec spark spark-submit --packages $SPARK_KAFKA_PACKAGE streaming/consumer_streaming.py mongo
```
Escribe en `discos.agregados_afr` (upsert por `ventana_inicio` + `model`).

## Notas
- Para repetir la corrida borra el checkpoint: `docker compose exec spark rm -rf /tmp/chk /tmp/chk_mongo` y, si quieres, recrea el tópico.
- Las ventanas de 7 días se alinean a la época Unix (no al 2026-04-01), así que la primera ventana puede ser parcial.
