package com.DataUtilityTransform.ExpressDataUtility.metric.step

import org.apache.spark.sql.{DataFrame, SQLContext}

trait StepAction {
  def dataFrameName: String
  def actOnDataFrame(sqlContext: SQLContext): DataFrame
  def getCachingEnableStatus(): String
}
