from confluent_kafka import Producer
import json
import random
import time
import uuid
from datetime import datetime, timezone

ESTUDIANTE = "RIVERA_ESTEBAN"
TOPIC = "rapidgo-telemetria-v2"
NUM_EVENTOS = 160
INTERVALO = 0.25
seed = sum((i + 1) * ord(c) for i, c in enumerate(ESTUDIANTE))
random.seed(seed)
run_id = uuid.uuid4().hex[:8]


producer = Producer({"bootstrap.servers": "localhost:9092"})
ciudades = ["Bogota", "Medellin", "Cali", "Barranquilla", "Bucaramanga"]
repartidores = list(range(1001, 1021))
centros = {
    "Bogota": (4.65, -74.08),
    "Medellin": (6.24, -75.57),
    "Cali": (3.45, -76.53),
    "Barranquilla": (10.98, -74.80),
    "Bucaramanga": (7.12, -73.12),
}

def delivery(err, msg):
    if err:
        print("ERROR:", err)
    else:
        print(f"OK p={msg.partition()} off={msg.offset()} key={msg.key().decode()}")
        print(f"ESTUDIANTE={ESTUDIANTE} RUN_ID={run_id} INTERVALO={INTERVALO}s")


for i in range(1, NUM_EVENTOS + 1):
    repartidor_id = random.choice(repartidores)
    ciudad = ciudades[(repartidor_id - 1001) % len(ciudades)]
    lat0, lon0 = centros[ciudad]
    estado = random.choices(
        ["disponible", "recogiendo", "en_entrega", "entregado", "cancelado"],
        weights=[15, 20, 40, 20, 5],
        k=1,
    )[0]
    evento = {
        "evento_id": f"{run_id}-{i:04d}",
        "estudiante": ESTUDIANTE,
        "run_id": run_id,
        "repartidor_id": repartidor_id,
        "pedido_id": f"PED-{random.randint(3000, 9999)}",
        "ciudad": ciudad,
        "latitud": round(lat0 + random.uniform(-0.05, 0.05), 6),
        "longitud": round(lon0 + random.uniform(-0.05, 0.05), 6),
        "velocidad_kmh": round(random.uniform(0, 90), 1),
        "bateria_pct": random.randint(5, 100),
        "distancia_restante_km": round(random.uniform(0.1, 18), 1),
        "estado": estado,
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
    }

    producer.produce(
        TOPIC,
        key=str(repartidor_id).encode("utf-8"),
        value=json.dumps(evento).encode("utf-8"),
        callback=delivery,
    )
    producer.poll(0)
    time.sleep(INTERVALO)
producer.flush()
print("FIN. RUN_ID:", run_id)
