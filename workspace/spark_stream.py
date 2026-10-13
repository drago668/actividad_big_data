from pyspark.sql import SparkSession, functions as F, types as T
from pathlib import Path
import time

ESTUDIANTE = "RIVERA_ESTEBAN"
TOPIC = "rapidgo-telemetria-v2"
KAFKA = "kafka:19092"
BASE = f"/opt/spark/work-dir/resultados/{ESTUDIANTE}"

spark = (SparkSession.builder
    .appName(f"RapidGoStreaming_{ESTUDIANTE}")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("evento_id", T.StringType()),
    T.StructField("estudiante", T.StringType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("repartidor_id", T.IntegerType()),
    T.StructField("pedido_id", T.StringType()),
    T.StructField("ciudad", T.StringType()),
    T.StructField("latitud", T.DoubleType()),
    T.StructField("longitud", T.DoubleType()),
    T.StructField("velocidad_kmh", T.DoubleType()),
    T.StructField("bateria_pct", T.IntegerType()),
    T.StructField("distancia_restante_km", T.DoubleType()),
    T.StructField("estado", T.StringType()),
    T.StructField("timestamp_evento", T.StringType()),
])

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load())

parsed = (raw
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_value",
        "topic", "partition", "offset",
        "timestamp AS kafka_timestamp")
    .withColumn("data", F.from_json("json_value", schema))
    .select("kafka_key", "topic", "partition", "offset", "kafka_timestamp", "data.*")
    .withColumn("event_time", F.to_timestamp("timestamp_evento"))
    .filter(F.col("estudiante") == ESTUDIANTE))

processed = (parsed
    .withColumn(
        "nivel_alerta",
        F.when((F.col("bateria_pct") < 10) | (F.col("estado") == "cancelado"), "CRITICA")
        .when((F.col("bateria_pct") < 20) |
            (F.col("velocidad_kmh") > 65) |
            ((F.col("estado") == "en_entrega") & (F.col("distancia_restante_km") > 12)), "ADVERTENCIA")
        .otherwise("NORMAL"))
    .withColumn(
        "causa_alerta",
        F.when(F.col("estado") == "cancelado", "PEDIDO_CANCELADO")
        .when(F.col("bateria_pct") < 10, "BATERIA_CRITICA")
        .when(F.col("bateria_pct") < 20, "BATERIA_BAJA")
        .when(F.col("velocidad_kmh") > 65, "VELOCIDAD_ALTA")
        .when((F.col("estado") == "en_entrega") & (F.col("distancia_restante_km") > 12), "POSIBLE_DEMORA")
        .otherwise("SIN_ALERTA")))

EVENTOS = f"{BASE}/eventos_procesados"
ALERTAS = f"{BASE}/alertas"
CP_EVENTOS = f"{BASE}/checkpoints/eventos"
CP_METRICAS = f"{BASE}/checkpoints/metricas"

def procesar_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()
    print(f"\n=== MICRO-BATCH {batch_id} ===")
    batch_df.groupBy("partition").agg(
        F.min("offset").alias("offset_min"),
        F.max("offset").alias("offset_max"),
        F.count("*").alias("eventos")
    ).orderBy("partition").show(truncate=False)

    batch_df.write.mode("append").parquet(EVENTOS)

    alertas = batch_df.filter(F.col("nivel_alerta") != "NORMAL")
    alertas.write.mode("append").parquet(ALERTAS)
    print("Alertas en el micro-batch:", alertas.count())
    batch_df.unpersist()

q_eventos = (processed.writeStream
    .foreachBatch(procesar_batch)
    .option("checkpointLocation", CP_EVENTOS)
    .trigger(processingTime="5 seconds")
    .start())

metricas = (processed
    .withWatermark("event_time", "30 seconds")
    .groupBy(F.window("event_time", "20 seconds"), "ciudad")
    .agg(
        F.count("*").alias("eventos"),
        F.round(F.avg("velocidad_kmh"), 2).alias("velocidad_promedio"),
        F.sum(F.when(F.col("bateria_pct") < 20, 1).otherwise(0)).alias("baterias_bajas"),
        F.sum(F.when(F.col("nivel_alerta") == "CRITICA", 1).otherwise(0)).alias("alertas_criticas")
    ))

q_metricas = (metricas.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", CP_METRICAS)
    .trigger(processingTime="10 seconds")
    .start())

try:
    while True:
        time.sleep(10)
        progreso = q_eventos.lastProgress
        if progreso:
            print("PROGRESO:", {
                "batchId": progreso.get("batchId"),
                "numInputRows": progreso.get("numInputRows"),
                "inputRowsPerSecond": progreso.get("inputRowsPerSecond"),
                "processedRowsPerSecond": progreso.get("processedRowsPerSecond"),
            })

except KeyboardInterrupt:
    print("Deteniendo consultas...")
finally:
    q_eventos.stop()
    q_metricas.stop()
    time.sleep(2)

    if Path(EVENTOS).exists():
        df_final = spark.read.parquet(EVENTOS)
        print("\n=== RESUMEN POR RUN_ID ===")
        df_final.groupBy("run_id").count().orderBy("run_id").show(truncate=False)

        print("\n=== ALERTAS POR NIVEL Y CAUSA ===")
        df_final.groupBy("nivel_alerta", "causa_alerta").count()
        df_final.groupBy("nivel_alerta", "causa_alerta").count().orderBy(F.desc("count")).show(50, False)

        print("\n=== RESUMEN POR CIUDAD ===")
        df_final.groupBy("ciudad").agg(
            F.count("*").alias("eventos"),
            F.round(F.avg("velocidad_kmh"), 2).alias("velocidad_promedio"),
            F.sum(F.when(F.col("nivel_alerta") == "CRITICA", 1).otherwise(0)).alias("criticas")
        ).orderBy(F.desc("criticas")).show(truncate=False)

    spark.stop()