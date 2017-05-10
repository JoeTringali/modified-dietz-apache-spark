package com.tricosoft.app

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

case class CashFlow(customerId: String, netExternalFlow: Double)

/**
 * @author ${user.name}
 */
class CalculateModifiedDietz(sc: SparkContext) 
{
  def run(balancePath: String, flowPath: String)
    : RDD[String] =
  {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val balanceByCustomerAndDateRDD = sc.textFile(balancePath)
      .map(_.split(","))
      .map(b=>((b(0),b(3)),b(5).toDouble))
      .reduceByKey(_ + _)
    val balanceByCustomerRDD = balanceByCustomerAndDateRDD
      .map(b=>(b._1._1, (b._1._2, b._2, b._1._2, b._2)))
      .reduceByKey((a,b)=>
              (if (a._1 < b._1) a._1 else b._1, 
               if (a._1 < b._1) a._2 else b._2, 
               if (a._3 > b._3) a._3 else b._3, 
               if (a._3 > b._3) a._4 else b._4))
    val flowByCustomerAndDateRDD = sc.textFile(flowPath)
      .map(_.split(","))
      .map(f=>((f(0),f(2)),f(5).toDouble))
      .reduceByKey(_ + _)
    val flowByCustomerRDD = flowByCustomerAndDateRDD
      .map(f=>(f._1._1,f._2))
      .reduceByKey(_ + _)
    val flowByCustomerAndDateByCustomerRDD = flowByCustomerAndDateRDD
      .map(f=>(f._1._1,(f._1._2, f._2)))
    val balanceByCustomerRowRDD = balanceByCustomerRDD
      .map(b=>Row(b._1, b._2._1, b._2._2, b._2._3, b._2._4))
    val balanceByCustomerRowRDDSchema = StructType(
        Array(
            StructField("customerId",StringType, true),
            StructField("beginningBalanceDate",StringType, true),
            StructField("beginningBalance",DoubleType,true),
            StructField("endingBalanceDate",StringType, true),
            StructField("endingBalance",DoubleType,true)))
    val balanceByCustomerDF = sqlContext
      .createDataFrame(balanceByCustomerRowRDD, balanceByCustomerRowRDDSchema)
    balanceByCustomerDF.createOrReplaceTempView("balanceByCustomer")
    val flowByCustomerRowRDD = flowByCustomerRDD
      .map(b=>Row(b._1, b._2))
    val flowByCustomerRowRDDSchema = StructType(
        Array(
            StructField("customerId",StringType, true),
            StructField("netFlow",DoubleType,true)))
    val flowByCustomerDF = sqlContext
      .createDataFrame(flowByCustomerRowRDD, flowByCustomerRowRDDSchema)
    flowByCustomerDF.createOrReplaceTempView("flowByCustomer")
    val flowByCustomerAndDateRowRDD = flowByCustomerAndDateRDD
      .map(b=>Row(b._1._1, b._1._2, b._2))
    val flowByCustomerAndDateRowRDDSchema = StructType(
        Array(
            StructField("customerId",StringType, true),
            StructField("cashFlowDate",StringType, true),
            StructField("cashFlow",DoubleType,true)))
    val flowByCustomerAndDateDF = sqlContext
      .createDataFrame(flowByCustomerAndDateRowRDD, flowByCustomerAndDateRowRDDSchema)
    flowByCustomerAndDateDF.createOrReplaceTempView("flowByCustomerAndDate")
    val results = sc.parallelize(sqlContext.sql(
        """SELECT b.customerId,
                  MIN(DATE(b.beginningBalanceDate)),
                  MIN(b.beginningBalance),
                  MAX(DATE(b.endingBalanceDate)),
                  MAX(b.endingBalance),
                  MAX(f.netFlow),
                  MAX(b.endingBalance - b.beginningBalance - f.netFlow),
                  SUM(
                    (
                      DATEDIFF(DATE(b.endingBalanceDate), DATE(b.beginningBalanceDate)) -
                      DATEDIFF(DATE(fd.cashFlowDate),DATE(b.beginningBalanceDate))
                    ) * 1.0 /
                    DATEDIFF(DATE(b.endingBalanceDate), DATE(b.beginningBalanceDate)) *
                    fd.cashFlow 
                  )
           FROM balanceByCustomer b 
           JOIN flowByCustomer f ON b.customerId = f.customerId 
           JOIN flowByCustomerAndDate fd ON f.customerId = fd.customerId 
           GROUP BY b.customerId 
           ORDER BY b.customerId""")
           .collect())
           .map(
               r=>r(0)+","+
               r(1)+","+
               r(2)+","+
               r(3)+","+
               r(4)+","+
               r(5)+","+
               r(6)+","+
               r(7)+","+
               (r(6).asInstanceOf[Double]/r(7).asInstanceOf[Double]))
    return results
  }
}

object CalculateModifiedDietz 
{
  def main(args : Array[String]) 
  {
    val balance = args(0)
    val flow = args(1)
    val config = new SparkConf().setAppName("CalculateModifiedDietz").setMaster("local")
    val context = new SparkContext(config)
    val job = new CalculateModifiedDietz(context)
    val results = job.run(balance, flow)
    val output = args(2)
    results.saveAsTextFile(output)
    context.stop()
  }
}
