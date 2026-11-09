from pyspark.sql import SparkSession


def build_session(app_name: str, **conf) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
