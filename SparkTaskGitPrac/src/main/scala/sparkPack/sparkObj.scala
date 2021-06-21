package sparkPack

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import sys.process._
import org.apache.spark.sql.types._

object sparkObj {
  def main(args:Array[String]):Unit={
        
        val conf = new SparkConf().setAppName("First").setMaster("local")
				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")
				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._
				
				
				val bookxmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///C:/Users/shikh/Documents/Zeyobron_Coaching/Data/book.xml")
	      bookxmldf.show()
	      bookxmldf.printSchema()
	      
	      val notexmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","note").load("file:///C:/Users/shikh/Documents/Zeyobron_Coaching/Data/note.xml")
	      notexmldf.show()
	      notexmldf.printSchema()
	      
	      val transxmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///C:/Users/shikh/Documents/Zeyobron_Coaching/Data/transactions.xml")
	      transxmldf.show()
	      transxmldf.printSchema()
	      
	      
	      
	      val rdbmsdata = spark.read.format("jdbc")
	        .option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/zeyodb")
	        .option("driver","com.mysql.jdbc.Driver")
	        .option("dbtable","web_customer")
	        .option("user","root")
	        .option("password","Aditya908")
	        .load()
	        
	      rdbmsdata.show(false)
	      
	     
	      
	      rdbmsdata.printSchema()
	     
	      
	      
	      
	      
	      
	      
	      
	      
	      
	      
	      
	      
	        
  }

}