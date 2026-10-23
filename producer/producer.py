"""Producer: publica los CSV diarios de Backblaze a Kafka simulando el paso del tiempo.

1 "día" (un CSV completo) = --day-seconds segundos reales (por defecto 10).
"""
import argparse
import csv
import json
import os
import time
import zlib
from pathlib import Path

from kafka import KafkaProducer

COLUMNS = ("date", "serial_number", "model", "failure")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", default=str(Path(__file__).resolve().parent.parent / "data"))
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                   help="Dentro de Docker: kafka:9092. Desde el host: localhost:29092")
    p.add_argument("--topic", default="discos-eventos")
    p.add_argument("--day-seconds", type=float, default=10.0, help="Pausa entre días (reloj acelerado)")
    p.add_argument("--models", default=None,
                   help="Lista separada por comas de modelos a conservar (3-5). Ej: 'ST4000DM000,HGST HMS5C4040BLE640'")
    p.add_argument("--sample", type=float, default=None,
                   help="Fracción 0-1 de discos a conservar (muestreo por serial_number, estable entre días)")
    p.add_argument("--max-rows-per-day", type=int, default=350_000,
                   help="Si un día supera este volumen y no hay --models/--sample, se muestrea automáticamente")
    return p.parse_args()


def count_rows(path):
    with open(path, newline="") as f:
        return max(sum(1 for _ in f) - 1, 0)


def keep_by_sample(serial, frac):
    # Hash estable por disco: el mismo disco entra o sale en todos los días.
    return (zlib.crc32(serial.encode()) % 10_000) < frac * 10_000


def main():
    args = parse_args()
    files = sorted(Path(args.data_dir).glob("*.csv"))  # nombres YYYY-MM-DD.csv => orden por fecha
    if not files:
        raise SystemExit(f"No hay CSV en {args.data_dir}")

    models = {m.strip() for m in args.models.split(",")} if args.models else None
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
        batch_size=64 * 1024,
    )

    total = 0
    for i, path in enumerate(files):
        n_file = count_rows(path)
        frac = args.sample
        if frac is None and models is None and n_file > args.max_rows_per_day:
            frac = args.max_rows_per_day / n_file
            print(f"[aviso] {path.name} tiene {n_file} filas > {args.max_rows_per_day}; muestreo automático {frac:.1%}")

        sent = 0
        print(f"=== Día {i + 1}/{len(files)}: {path.name} ({n_file} filas en archivo) ===")
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                if models is not None and row["model"] not in models:
                    continue
                if frac is not None and not keep_by_sample(row["serial_number"], frac):
                    continue
                msg = {
                    "date": row["date"],
                    "serial_number": row["serial_number"],
                    "model": row["model"],
                    "failure": int(row["failure"]),
                }
                producer.send(args.topic, key=msg["serial_number"], value=msg)
                sent += 1
                if sent % 50_000 == 0:
                    print(f"  {path.name}: {sent} filas publicadas...")
        producer.flush()
        total += sent
        print(f"  Día {path.name} completo: {sent} filas (acumulado: {total})")

        if i < len(files) - 1:
            print(f"  Esperando {args.day_seconds:g}s para el siguiente día...")
            time.sleep(args.day_seconds)

    producer.close()
    print(f"Listo. {total} mensajes publicados en '{args.topic}'.")


if __name__ == "__main__":
    main()
