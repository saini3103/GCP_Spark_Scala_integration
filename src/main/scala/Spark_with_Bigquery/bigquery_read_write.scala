package Spark_with_Bigquery
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object bigquery_read_write extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","Read Bigquery Table in Spark")
  //sparkConf.set("master","yarn")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  println("\nLoading the Bigquery Table\n")

  val orders = spark.read
    .format("bigquery")
    .option("table","ascendant-yeti-416817.raw_data.orders")
    .load()

  println("\n The orders table is loaded successfully from bigquery\n")
  println("\nThe loaded table look like this :\n")
  orders.show(5,false)

  val tmpBucket = "dataproc-temp-asia-east1-6154883603-3rdbdx7m"

  spark.conf.set("temporaryGcsBucket", tmpBucket)

  val projectId = "ascendant-yeti-416817"
  val datasetId = "raw_data"

  orders.write
    .format("bigquery")
    .option("project",projectId)
    .option("dataset",datasetId)
    .option("table","ascendant-yeti-416817.raw_data.order_write2")
    .mode(SaveMode.Append)
    .save()

  spark.stop()
}