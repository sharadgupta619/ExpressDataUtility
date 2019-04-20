package com.DataUtilityTransform.ExpressDataUtility.metric.config

import java.io.File

import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.fasterxml.jackson.annotation.JsonProperty

case class Step(@JsonProperty val sql: Option[String], @JsonProperty val repartition: Option[String], @JsonProperty val file: Option[String], @JsonProperty stepName: String,
                @JsonProperty val enableCaching: Option[String]) {

  def getSqlQuery(metricDir: File): String = {
    //TODO: NoSuchFile exception
    sql match {
      case Some(expression) => expression
      case None => {
        file match {
          case Some(filePath) => FileUtils.getContentFromFileAsString(new File(metricDir, filePath))
          case None => ""
        }
      }
    }
  }

  def getPartitions(): String = {
    repartition match {
      case Some(value) => value
      case None => ""
    }
  }

  def getCachingEnableStatus(): String = {
    enableCaching match {
      case Some(value) => value
      case None => ""
    }
  }
}
