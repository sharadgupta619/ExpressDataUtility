package com.DataUtilityTransform.ExpressDataUtility.metric

import java.io.{File, FileReader}

import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityInvalidMetricFileException
import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityInvalidMetricFileException
import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityInvalidMetricFileException
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils

object MetricFile {
  val validExtensions = Seq("json", "yaml")

  def isValidFile(path: File): Boolean = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }
}

class MetricFile(path: File) {
  val log: Logger = LogManager.getLogger(this.getClass)

  val fileName: String = path.getName
  val metricDir: File = path.getParentFile

  log.info(s"Initializing Metric file $fileName")
  val metric: Metric = getMetric

  private def getMetric = {
    try {
      val metricConfig = parseFile(path.getAbsolutePath)
      new Metric(metricConfig, metricDir, FilenameUtils.removeExtension(fileName))
    } catch {
      case e: Exception => throw ExpressDataUtilityInvalidMetricFileException(s"Failed to parse metric file $fileName", e)
    }
  }

  def parseFile(fileName: String): MetricConfig = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(fileName), classOf[MetricConfig])
      }
      case None => throw ExpressDataUtilityInvalidMetricFileException(s"Unknown extension for file $fileName")
    }
  }
}
