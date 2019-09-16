package com.assignment.spark

import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.sql.functions._
import com.assignment.spark.bo.TonnageConst._
import com.assignment.spark.bo.domainConst._

object tonnageCountHits {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().enableHiveSupport().appName("tonnageCountHits").getOrCreate()
    logger.info("Application ID -> " + spark.sparkContext.applicationId)
    val inputDf = spark.sql("select * from rohan_test.edr_data_hive")
    val filteredDf = getFilteredDf(inputDf)
    val udf_func : String => String = _.split("/")(2)
    val domain = udf(udf_func)
    val temp_df = filteredDf.withColumn(getDomainName,domain(col(gethttpUrl)))
    val final_df = temp_df.groupBy(getDomainName,"hour","minute").
      agg(sum(col(getUplink)).as(getSumUplink),sum(col(getDownLink)).as(getSumDownlink),count(getReplyCodeCol).as(getHitsCol)).
      drop(getUplink,getDownLink).
      withColumn(getSumTotalBytes,col(getSumUplink)+col(getSumDownlink))

    final_df.write.partitionBy("hour","minute").mode("append").format("orc").saveAsTable("rohan_output.tonnage_count_hits_table")
  }
  def getFilteredDf (dataFrame: DataFrame) : DataFrame = {
    dataFrame.filter(col(gethttpUrl).rlike("^http://(?!/)..*$|^https://(?!/)..*$")).filter(col(getReplyCodeCol).rlike("^2[0-9][0-9]$"))
  }

}
