import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
/**
  * Created by neha on 17/1/17.
  */

/*
size of broadcast shouldn't exceed 2gb
data is sent to all the nodes
 */
object DataframeEx{
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setMaster("local[*]").setAppName("DataframeTry")
    val sc= new SparkContext(conf)
    val sqlContext= new SQLContext(sc)
    val personDemographucCSVPAth= "/home/neha/Desktop/SparkTest/src/main/resources/dataframetry/person-demo.csv"
    val readDF= sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(personDemographucCSVPAth)
    readDF.show()

    val personHealthCSVPath= "/home/neha/Desktop/SparkTest/src/main/resources/dataframetry/person-health.csv"
    val personHealthDF= sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(personHealthCSVPath)
    personHealthDF.show()

    /* 2 ways to do the join
    health data will be bigger than patient demographic data
    so huge data will be kept on left
    so method 2 will be used for better speed
     */

    //Method 1
//    val personDF= readDF.join(personHealthDF, personHealthDF("id")=== readDF("id"), "left_outer")
//    personDF.show()

    //Method 2
    val personDF2= personHealthDF.join(broadcast(readDF), personHealthDF("id")===readDF("id"), "right_outer")
      .drop(personHealthDF("id"))
    personDF2.show()

    val ageLessThan50DF= personDF2.filter(personDF2("age")<50)
    ageLessThan50DF.show()

    ageLessThan50DF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)//optional
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/home/neha/Desktop/SparkTest/dataframetry/many")


    val insuranceDF= sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("/home/neha/Desktop/SparkTest/src/main/resources/dataframetry/person-insurance.csv")
    insuranceDF.show()

    val personInsuranceDF= insuranceDF.join(personDF2, personDF2("id")===insuranceDF("id"), "left_outer" )
      .drop(personDF2("id"))
    personInsuranceDF.show()

    val format = new SimpleDateFormat("YYYY-MM-DD")
    val currDate = format.format(Calendar.getInstance().getTime())
    val validDateDF= personInsuranceDF.filter(personInsuranceDF("datevalidation")>currDate)
    val payerwisePayerDF= validDateDF.groupBy("payer").agg(sumDistinct("amount"))

    payerwisePayerDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/home/neha/Desktop/SparkTest/dataframetry/payerwiseSum")

    payerwisePayerDF.show()
  }
}
