package example

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.mllib.stat.Statistics

object Stock {

  case class Stock(dt: String, openprice: Double, highprice: Double, lowprice: Double, closeprice: Double, volume: Double, adjcloseprice: Double)
  // function to parse input into Stock class  
  def parseStock(str: String): Stock = {
    val line = str.split(",")
    Stock(line(0), line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble)
  }
  def parseRDD(rdd: RDD[String]): RDD[Stock] = {
    val header = rdd.first
    rdd.filter(_(0) != header(0)).map(parseStock).cache()
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    var filename = "spytable.csv"
    val stocksRDD = parseRDD(sc.textFile(filename)).cache()
    val stocksDF = stocksRDD.toDF().cache()
    stocksDF.registerTempTable("spy")
    stocksDF.printSchema

    // COMMAND ----------

    stocksRDD.take(1)

    // COMMAND ----------

    filename = "apctable.csv"
    val astocksRDD = parseRDD(sc.textFile(filename)).cache()
    val astocksDF = astocksRDD.toDF().cache
    astocksDF.registerTempTable("apc")
    filename = "xomtable.csv"
    val estocksRDD = parseRDD(sc.textFile(filename)).cache()
    val estocksDF = estocksRDD.toDF().cache
    estocksDF.registerTempTable("xom")

    // COMMAND ----------

    estocksDF.show

    // COMMAND ----------


    estocksDF.show
    // COMMAND ----------

    estocksDF.schema

    // COMMAND ----------

    astocksDF.show

    // COMMAND ----------

    //
    stocksDF.select($"dt", $"adjcloseprice".alias("sclose")).show

    // COMMAND ----------

    // Compute the average closing price per year for SPY 
    stocksDF.select(year($"dt").alias("yr"), $"adjcloseprice").groupBy("yr").avg("adjcloseprice").orderBy(desc("yr")).show

    // COMMAND ----------

    // Compute the average closing price per year for Exxon XOM
    estocksDF.select(year($"dt").alias("yr"), $"adjcloseprice").groupBy("yr").avg("adjcloseprice").orderBy(desc("yr")).show

    // COMMAND ----------

    astocksDF.select(year($"dt").alias("yr"), month($"dt").alias("mo"), $"adjcloseprice").groupBy("yr", "mo").agg(avg("adjcloseprice")).orderBy(desc("yr"), desc("mo")).show

    // COMMAND ----------

    astocksDF.select(year($"dt").alias("yr"), month($"dt").alias("mo"), $"adjcloseprice").groupBy("yr", "mo").agg(avg("adjcloseprice")).orderBy(desc("yr"), desc("mo")).explain

    // COMMAND ----------

    // List and also count the number of times the closing price for SPY went up or down by more than 4 
    var res = sqlContext.sql("SELECT spy.dt, spy.openprice, spy.closeprice, abs(spy.closeprice - spy.openprice) as spydif FROM spy WHERE abs(spy.closeprice - spy.openprice) > 4 ")
    res.show
    res.count



    sqlContext.sql("SELECT spy.dt, spy.openprice, spy.closeprice, abs(spy.closeprice - spy.openprice) as spydif FROM spy WHERE abs(spy.closeprice - spy.openprice) > 4 ").explain


    // 
    sqlContext.sql("SELECT spy.dt, abs(spy.closeprice - spy.openprice) as spydif, xom.dt, abs(xom.closeprice - xom.openprice) as xomdif FROM spy join xom on spy.dt = xom.dt  WHERE (abs(spy.closeprice - spy.openprice) > 2 and abs(xom.closeprice - xom.openprice) > 2)").explain

    // COMMAND ----------

    sqlContext.sql("SELECT spy.dt, abs(spy.closeprice - spy.openprice) as spydif, xom.dt, abs(xom.closeprice - xom.openprice) as xomdif FROM spy join xom on spy.dt = xom.dt  WHERE (abs(spy.closeprice - spy.openprice) > 2 and abs(xom.closeprice - xom.openprice) > 2)").show


    sqlContext.sql("SELECT spy.dt, spy.closeprice, xom.closeprice FROM spy join xom on spy.dt = xom.dt  WHERE (spy.closeprice  > 200 and xom.closeprice > 80)").explain

    sqlContext.sql("SELECT year(spy.dt) as yr, max(spy.adjcloseprice), min(spy.adjcloseprice), max(xom.adjcloseprice), min(xom.adjcloseprice) FROM spy join xom on spy.dt = xom.dt  group By year(spy.dt)").show


    sqlContext.sql("SELECT year(spy.dt) as yr, max(spy.adjcloseprice), min(spy.adjcloseprice), max(xom.adjcloseprice), min(xom.adjcloseprice) FROM spy join xom on spy.dt = xom.dt  group By year(spy.dt)").explain



    sqlContext.sql("SELECT spy.dt, abs(spy.closeprice - spy.openprice) as spydif, xom.dt, abs(xom.closeprice - xom.openprice) as xomdif FROM spy join xom on spy.dt = xom.dt  WHERE (abs(spy.closeprice - spy.openprice) > 2 and abs(xom.closeprice - xom.openprice) > 2)").show


    // Compute the average closing price per year for SPY 
    stocksDF.select(year($"dt").alias("yr"), $"adjcloseprice").groupBy("yr").avg("adjcloseprice").orderBy(desc("yr")).show


    // Compute the average closing price per year for SPY 
    stocksDF.select(year($"dt").alias("yr"), $"adjcloseprice").groupBy("yr").avg("adjcloseprice").orderBy(desc("yr")).show


    // Calculate and display the average closing price per month for XOM ordered by year,month (most recent ones should be displayed first)
    sqlContext.sql("SELECT year(xom.dt) as yr, month(xom.dt) as mo,  avg(xom.adjcloseprice) as xomavgclose from xom group By year(xom.dt), month(xom.dt) order by year(xom.dt) desc, month(xom.dt) desc").show
    // Calculate and display the average closing price per month for SPY ordered by year,month (most recent ones should be displayed first)
    sqlContext.sql("SELECT year(spy.dt) as yr, month(spy.dt) as mo,  avg(spy.adjcloseprice) as spyavgclose from spy group By year(spy.dt), month(spy.dt) order by year(spy.dt) desc, month(spy.dt) desc").show


    // Calculate and display the average closing price per month for XOM ordered by year,month (most recent ones should be displayed first)
    sqlContext.sql("SELECT year(apc.dt) as yr, month(apc.dt) as mo,  avg(apc.adjcloseprice) as apcavgclose from apc group By year(apc.dt), month(apc.dt) order by year(apc.dt) desc, month(apc.dt) desc").show

 
    // Join all stocks so as to compare the closing prices 
    val joinclose = sqlContext.sql("SELECT apc.dt, apc.adjcloseprice as apcclose, spy.adjcloseprice as spyclose, xom.adjcloseprice as xomclose from apc join spy on apc.dt = spy.dt join xom on spy.dt = xom.dt").cache
    joinclose.show
    joinclose.registerTempTable("joinclose")

    sqlContext.sql("SELECT apc.dt, apc.adjcloseprice as apcclose, spy.adjcloseprice as spyclose, xom.adjcloseprice as xomclose from apc join spy on apc.dt = spy.dt join xom on spy.dt = xom.dt").explain

    joinclose.explain



    //  compare year average closing prices 
    sqlContext.sql("SELECT year(joinclose.dt) as yr, avg(joinclose.apcclose) as avgapcclose, avg(joinclose.spyclose) as avgspyclose, avg(joinclose.xomclose) as avgxomclose from joinclose group By year(joinclose.dt) order by year(joinclose.dt)").show
  

    // write to a parquet table 
    joinclose.write.format("parquet").save("joinstock.parquet")

    // COMMAND ----------

    val df = sqlContext.read.parquet("joinstock.parquet")


    df.show
    df.printSchema

    df.explain()

    // COMMAND ----------

    //var agg_df = df.groupBy("location").agg(min("id"), count("id"), avg("date_diff"))
    df.select(year($"dt").alias("yr"), month($"dt").alias("mo"), $"apcclose", $"xomclose", $"spyclose").groupBy("yr", "mo").agg(avg("apcclose"), avg("xomclose"), avg("spyclose")).orderBy(desc("yr"), desc("mo")).show

    // COMMAND ----------

    df.select(year($"dt").alias("yr"), month($"dt").alias("mo"), $"apcclose", $"xomclose", $"spyclose").groupBy("yr", "mo").agg(avg("apcclose"), avg("xomclose"), avg("spyclose")).orderBy(desc("yr"), desc("mo")).explain

    // COMMAND ----------

    // http://www.brookings.edu/blogs/ben-bernanke/posts/2016/02/19-stocks-and-oil-prices 
    var seriesX = df.select($"xomclose").map { row: Row => row.getAs[Double]("xomclose") } //.rdd
    var seriesY = df.select($"spyclose").map { row: Row => row.getAs[Double]("spyclose") } //.rdd
    var correlation = Statistics.corr(seriesX, seriesY, "pearson")

    // COMMAND ----------

    seriesX = df.select($"apcclose").map { row: Row => row.getAs[Double]("apcclose") } //.rdd
    seriesY = df.select($"xomclose").map { row: Row => row.getAs[Double]("xomclose") } //.rdd
    correlation = Statistics.corr(seriesX, seriesY, "pearson")
    
  }
}

