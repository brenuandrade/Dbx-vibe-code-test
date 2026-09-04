"""Simulação de estresse operacional para testar a resiliência do pipeline
antes de eventos de alta carga como a Black Friday.

Duas categorias de cenário, comuns nesse tipo de evento:

- **Data skew**: uma fração pequena de chaves (um produto "viral" de
  campanha-relâmpago, uma loja com oferta exclusiva) concentra grande parte
  do volume. Em Spark, isso gera partições de shuffle muito maiores que as
  demais em operações como ``groupBy``/``join`` por essa chave — a causa
  clássica de tasks "penduradas" (stragglers) e OOM em jobs que rodavam bem
  com tráfego normal.
- **Picos de ingestão**: uma janela curta de tempo (ex.: abertura das
  ofertas à meia-noite) recebe um volume muito acima da média, estressando
  pipelines near-time — filas se acumulam e a latência de ingestão aumenta.

Use estas funções sobre uma amostra do dataset sintético (Silver/Gold) para
validar se as agregações da camada Gold — e o cluster/autoscaling do job —
aguentam esses cenários antes do dia real.
"""

from __future__ import annotations

import random

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def simulate_data_skew(
    df: DataFrame,
    key_column: str,
    hot_key_fraction: float = 0.05,
    replication_factor: int = 20,
    seed: int = 42,
) -> DataFrame:
    """Concentra o volume de dados em uma pequena fração das chaves de
    ``key_column``, simulando um "hot key" (produto viral, loja em
    destaque) sob a ótica de particionamento do Spark.

    Sorteia ``hot_key_fraction`` dos valores distintos de ``key_column``
    como "quentes" e replica suas linhas ``replication_factor`` vezes,
    mantendo as demais linhas intactas — o resultado tem poucas chaves com
    volume muito acima da média, útil para testar `groupBy`/`join` por essa
    coluna sob desbalanceamento real.

    Requer coletar os valores distintos de ``key_column`` para sortear as
    chaves quentes — adequado para amostras usadas em teste de carga, não
    para rodar sobre o volume total de produção.
    """
    distinct_keys = [row[key_column] for row in df.select(key_column).distinct().collect()]
    if not distinct_keys:
        return df

    rng = random.Random(seed)
    rng.shuffle(distinct_keys)
    n_hot = max(1, int(len(distinct_keys) * hot_key_fraction))
    hot_keys = distinct_keys[:n_hot]

    is_hot = F.col(key_column).isin(hot_keys)
    hot_df = df.filter(is_hot)
    cold_df = df.filter(~is_hot)

    # Replica as linhas das chaves quentes via explode — sem UDF, e sem
    # depender de amostragem aleatória adicional (replicação exata).
    replicated_hot_df = hot_df.withColumn(
        "_replica", F.explode(F.array(*[F.lit(i) for i in range(replication_factor)]))
    ).drop("_replica")

    return replicated_hot_df.unionByName(cold_df)


def simulate_ingestion_spike(
    df: DataFrame,
    timestamp_column: str,
    spike_start: str,
    spike_duration_minutes: int = 5,
    spike_multiplier: int = 15,
    extra_lag_seconds: int = 120,
) -> DataFrame:
    """Concentra um pico de volume em uma janela curta de tempo, simulando a
    explosão de tráfego típica da abertura de uma campanha (ex.: meia-noite
    da Black Friday).

    As linhas cujo ``timestamp_column`` cai dentro de
    ``[spike_start, spike_start + spike_duration_minutes)`` são replicadas
    ``spike_multiplier`` vezes; as demais permanecem inalteradas. Se o
    DataFrame tiver uma coluna ``source_ingested_at`` (latência de ingestão
    near-time — ver ``bronze.synthetic_data``), o pico também recebe um
    atraso extra (``extra_lag_seconds``), refletindo o backlog de fila que
    aparece sob carga muito acima do normal.
    """
    spike_start_ts = F.to_timestamp(F.lit(spike_start))
    spike_end_ts = spike_start_ts + F.expr(f"INTERVAL {spike_duration_minutes} MINUTES")
    ts_col = F.col(timestamp_column).cast("timestamp")
    in_spike_window = (ts_col >= spike_start_ts) & (ts_col < spike_end_ts)

    spike_df = df.filter(in_spike_window)
    outside_spike_df = df.filter(~in_spike_window)

    replicated_spike_df = spike_df.withColumn(
        "_replica", F.explode(F.array(*[F.lit(i) for i in range(spike_multiplier)]))
    ).drop("_replica")

    if "source_ingested_at" in df.columns:
        replicated_spike_df = replicated_spike_df.withColumn(
            "source_ingested_at",
            (
                F.col("source_ingested_at").cast("timestamp")
                + F.expr(f"INTERVAL {extra_lag_seconds} SECONDS")
            ).cast("string"),
        )

    return replicated_spike_df.unionByName(outside_spike_df)
