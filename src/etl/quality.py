from pyspark.sql import functions as F

def run_quality_checks(df) -> None:
    """
    Lança Exception se algum check falhar.
    """

    # Evita DF vazio
    if df.limit(1).count() == 0:
        raise ValueError("DQ_FAIL: dataframe is empty")

    # Timestamp não pode ser nulo
    null_ts = df.filter(F.col("ts_utc").isNull()).count()
    if null_ts > 0:
        raise ValueError(f"DQ_FAIL: ts_utc nulls={null_ts}")

    # Garante umidade entre 0 e 100 (se não nula)
    bad_h = df.filter(
        F.col("relative_humidity_2m").isNotNull() &
        ((F.col("relative_humidity_2m") < 0) | (F.col("relative_humidity_2m") > 100))
    ).count()
    if bad_h > 0:
        raise ValueError(f"DQ_FAIL: humidity out of range rows={bad_h}")

    # Garante precipitação não negativa
    bad_p = df.filter(
        F.col("precipitation").isNotNull() & (F.col("precipitation") < 0)
    ).count()
    if bad_p > 0:
        raise ValueError(f"DQ_FAIL: precipitation negative rows={bad_p}")

    # Garante temperatura em range plausível (ajustável)
    bad_t = df.filter(
        F.col("temperature_2m").isNotNull() &
        ((F.col("temperature_2m") < -80) | (F.col("temperature_2m") > 60))
    ).count()
    if bad_t > 0:
        raise ValueError(f"DQ_FAIL: temperature out of range rows={bad_t}")
