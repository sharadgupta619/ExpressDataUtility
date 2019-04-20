package com.DataUtilityTransform.ExpressDataUtility.metric.step

import com.DataUtilityTransform.ExpressDataUtility.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
  * Represents the SQL query to run
  */
case class Sql(query: String, dataFrameName: String, partitions: String, enableCaching: String) extends StepAction {
  val log = LogManager.getLogger(this.getClass)

  override def actOnDataFrame(sqlContext: SQLContext): DataFrame = {
    var partitionedDF = sqlContext.emptyDataFrame
    val newDf = sqlContext.sql(query)
    if (partitions != null && !partitions.isEmpty) {
      if (partitions forall Character.isDigit) {
        log.info(s"Repartitioning ${dataFrameName} on ${partitions} partitions")
        partitionedDF = newDf.repartition(partitions.toInt)
      } else {
        partitionedDF = newDf.repartition(newDf.col(partitions))
        log.info(s"Repartitioning ${dataFrameName} on ${partitions} column")
      }
    } else {
      partitionedDF = newDf
    }
    //newDf.persist(StorageLevel.MEMORY_AND_DISK)
    partitionedDF.createOrReplaceTempView(dataFrameName)
    partitionedDF
  }

  override def getCachingEnableStatus(): String = {
    enableCaching
  }
}
