import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object TestRun extends App{

  val unitType = println("Scala is working fine!")
  val a = 5
  val b = 3.14
  val c = 'e'
  def doubler (x:Int): Int ={
    x*2
  }

  println(doubler(5)  + "\t"+ a +"\t"+b+"\t"+c)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","Spark Session Test")
  sparkConf.set("spark.master","local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR) //org is the root package

  /*
  *.setLevel(Level.ERROR): This sets the logging level of the logger obtained in the previous step to "ERROR".
  * This means that only log messages with a severity level of "ERROR" or higher will be output,
  * while messages with lower severity levels like "WARN", "INFO", or "DEBUG" will be suppressed.
   */

  val myList = List(
    (1,"Rahul",29,125401,"2024-03-18"),
    (2,"Mukesh",35,110142,"2024-03-18"),
    (3,"Amit",32,142563,"2024-03-18"),
    (4,"Rajeev",25,25468,"2024-03-18"),
    (5,"Sudhir",38,85620,"2024-03-18"))

  val df1 = spark.createDataFrame(myList).toDF("id","name","age","salary","joining_date")

  df1.select(col("id"),col("name")).show()

  val schema = StructType(Seq(
    StructField("order_id", IntegerType, nullable = false),
    StructField("order_date", StringType, nullable = false),
    StructField("order_customer_id", IntegerType, nullable = false),
    StructField("order_status", StringType, nullable = false)
  ))

  val data = Seq(
    (1, "00:00.0", 11599, "CLOSED"),
    (2, "00:00.0", 256, "PENDING_PAYMENT"),
    (3, "00:00.0", 12345, "PROCESSING"),
    (4, "00:00.0", 67890, "COMPLETE"),
    (5, "00:00.0", 54321, "PENDING"),
    (6, "00:00.0", 98765, "CLOSED"),
    (6, "00:00.0", 98765, "CLOSED"),
    (6, "00:00.0", 98765, "CLOSED"),
    (6, "00:00.0", 98765, "CLOSED"),
    (6, "00:00.0", 98765, "CLOSED"),
    (7, "00:00.0", 11111, "PENDING_PAYMENT"),
    (8, "00:00.0", 22222, "PROCESSING"),
    (9, "00:00.0", 33333, "COMPLETE"),
    (10, "00:00.0", 44444, "PENDING"),
    (11, "00:00.0", 55555, "CLOSED"),
    (12, "00:00.0", 66666, "PENDING_PAYMENT")
  )

  val df5 = spark.createDataFrame(data).toDF("order_id","order_date","order_customer_id","order_status")

  df5.show()

  df5.select("order_customer_id").groupBy("order_customer_id").count().show()

  val df6 = spark.createDataFrame(data).toDF(schema.fieldNames: _*)
  df6.show()

  spark.stop()

}