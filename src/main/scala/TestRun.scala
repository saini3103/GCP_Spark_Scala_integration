import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

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


}
