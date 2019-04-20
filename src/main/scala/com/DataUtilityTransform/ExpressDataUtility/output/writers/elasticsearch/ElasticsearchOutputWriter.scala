package com.DataUtilityTransform.ExpressDataUtility.output.writers.elasticsearch

import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

class ElasticsearchOutputWriter(props: Map[String, String]) extends MetricOutputWriter {

  case class EsOutputProperties(documentName: String)

  @transient val log = LogManager.getLogger(this.getClass)

  val inputProperties = EsOutputProperties(props("documentName"))

  override def write(dataFrame: DataFrame): Unit = {

    println("Writing data to Elastic Search")
    dataFrame.saveToEs(inputProperties.documentName)
  }
}
