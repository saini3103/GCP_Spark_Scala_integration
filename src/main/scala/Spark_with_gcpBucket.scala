import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Spark_with_gcpBucket extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","GCP Bucket read and Write using Spark")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  println("\nLoading the file from gcp bucket\n")

  val customers = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","gs://input3103/customers-201025-223502.csv")
    .load()

  println("\nFile successfully loaded to the dataframe from bucket\n")

  println("\nThe Dataframe look like this: \n")

  customers.show(5,false)

  println("\n Writing the Dataframe into GCS Bucket \n")

     customers.write
    .mode(SaveMode.Append)
    .parquet("output3103")

}
