import org.apache.spark.sql.SparkSession

/**
  * Created by sonu on 8/8/16.
  */
object FireService extends App {

  val sparkSession = SparkSession.builder().appName("Assignment").master("local").getOrCreate()

  import sparkSession.implicits._

  val df = sparkSession.read.option("header","true").csv("/home/sonu/workspace/spark-assignment2/Fire_Department_Calls_for_Service.csv")

  // Q1​. How many different types of calls were made to the Fire Department ?
  val distinctCalls = df.select("Call Type").distinct().count()
  println("Total number of different type of calls were " + distinctCalls)

  // Q2​. How many incidents of each call type were there ?
  val incidentOfCallTypes = df.select("Incident Number", "Call Type").as[(String,String)].groupBy("Call Type").count()
      .as[(String,String)].collect().toList.toMap
  incidentOfCallTypes.foreach(println)

  // Q3​. How many years of Fire Service Calls in the data file ?
  val callDate = df.select("Call Date").as[String].distinct().collect().toList.map{
    elem => elem.split("/")(2).toInt
  }
  val sortedCallDate = callDate.sorted
  val totalNumberOfYears = sortedCallDate(sortedCallDate.length - 1) - sortedCallDate(0)
  println("The total number of years of data is " + totalNumberOfYears)

  // Q4​. How many service calls were logged in the last 7 days ?
  val distinctDateList = df.select("Call Date").as[String].distinct().collect().toList.sorted
  val latestDate = distinctDateList(distinctDateList.length - 1)
  println("latest >>> " + latestDate)
  val sevenDayOldDate = distinctDateList(distinctDateList.length - 8)
  val countOfSevenDays = df.select("Call Date").as[String].collect().toList.count(_ >= sevenDayOldDate)
  println("Calls in last seven days " + countOfSevenDays)

}