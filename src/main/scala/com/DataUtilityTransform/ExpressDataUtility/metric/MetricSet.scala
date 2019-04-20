package com.DataUtilityTransform.ExpressDataUtility.metric

import com.DataUtilityTransform.ExpressDataUtility.calculators.SqlStepCalculator
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityWriteFailedException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriterFactory
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.DataUtilityTransform.ExpressDataUtility.calculators.SqlStepCalculator
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityWriteFailedException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriterFactory
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.DataUtilityTransform.ExpressDataUtility.calculators.SqlStepCalculator
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityWriteFailedException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriterFactory
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.SparkGauge
import org.apache.spark.storage.StorageLevel

object MetricSet {
  type metricSetCallback = (String) => Unit
  private var beforeRun: Option[metricSetCallback] = None
  private var afterRun: Option[metricSetCallback] = None

  def setBeforeRunCallback(callback: metricSetCallback) {
    beforeRun = Some(callback)
  }

  def setAfterRunCallback(callback: metricSetCallback) {
    afterRun = Some(callback)
  }
}

class MetricSet(metricSet: String) {
  val log = LogManager.getLogger(this.getClass)

  val metrics: Seq[Metric] = parseMetrics(metricSet)

  def parseMetrics(metricSet: String): Seq[Metric] = {
    log.info(s"Starting to parse metricSet")
    val metricsToCalculate = FileUtils.getListOfFiles(metricSet)
    metricsToCalculate.filter(MetricFile.isValidFile(_)).map(new MetricFile(_).metric)
  }

  def run() {
    MetricSet.beforeRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }

    metrics.foreach(metric => {
      lazy val timer = InstrumentationUtils.createNewGauge(Array(metric.name, "timer"))
      val startTime = System.nanoTime()

      val calculator = new SqlStepCalculator(metric)
      calculator.calculate()
      val insertTable = write(metric)
      if (insertTable) {
        truncateIntermediateTables(metric)
      }

      val endTime = System.nanoTime()
      val elapsedTimeInNS = (endTime - startTime)
      timer.set(elapsedTimeInNS)
    })

    MetricSet.afterRun match {
      case Some(callback) => callback(metricSet)
      case None =>
    }
  }

  def write(metric: Metric): Boolean = {
    var insertTable = false
    metric.outputs.foreach(output => {
      val sparkSession = Session.getSparkSession
      val dataFrameName = output.outputConfig.stepName
      val dataFrame = sparkSession.table(dataFrameName)
      //dataFrame.persist(StorageLevel.MEMORY_AND_DISK)

      lazy val counterNames = Array(metric.name, dataFrameName, output.outputConfig.outputType.toString, "counter")
      lazy val dfCounter: SparkGauge = InstrumentationUtils.createNewGauge(counterNames)
      //dfCounter.set(dataFrame.count())

      log.info(s"Starting to Write results of ${dataFrameName}")
      try {
        output.writer.write(dataFrame)
        insertTable = true
      } catch {
        case ex: Exception => {
          insertTable = false
          throw ExpressDataUtilityWriteFailedException(s"Failed to write dataFrame: " +
            s"$dataFrameName to output: ${output.outputConfig.outputType} on metric: ${metric.name}", ex)
        }
      }
    })
    insertTable
  }

  def truncateIntermediateTables(metric: Metric): Unit = {
    metric.outputs.foreach(output => {
      if (output.outputConfig.outputType.equalsIgnoreCase("Hive")) {
        val hiveWriter: HiveOutputWriter = MetricOutputWriterFactory.getHiveWriter(output.outputConfig, metric.name)
        val sparkSession = Session.getSparkSession
        val dataFrameName = output.outputConfig.stepName
        val dataFrame = sparkSession.table(dataFrameName)
        hiveWriter.truncate(dataFrame)
      }
    })
  }


}
