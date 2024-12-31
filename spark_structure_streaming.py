# Условие: используйте источник rate, напишите код, который создаст дополнительный столбец, который будет выводить сумму только нечётных чисел.


from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("RateStreamSumOddNumbers").getOrCreate()

rate_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

windowed_df = rate_df.groupBy(f.window("timestamp", "1 minute")).agg(f.sum(f.when(f.col("value") % 2 != 0, f.col("value")).otherwise(0)).alias("Sum_Odd_Rates"))

query = windowed_df.writeStream.outputMode("update").format("console").start()
query.awaitTermination()