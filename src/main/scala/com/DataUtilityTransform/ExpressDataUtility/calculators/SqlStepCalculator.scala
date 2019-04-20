package com.DataUtilityTransform.ExpressDataUtility.calculators

import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityFailedStepException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.metric.Metric
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityFailedStepException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.metric.Metric
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityFailedStepException
import com.DataUtilityTransform.ExpressDataUtility.instrumentation.InstrumentationUtils
import com.DataUtilityTransform.ExpressDataUtility.metric.Metric
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class SqlStepCalculator(metric: Metric) extends Calculator {
  val log = LogManager.getLogger(this.getClass)
  lazy val successStepsCounter = InstrumentationUtils.createNewCounter(Array(metric.name, "successfulSteps"))
  lazy val failedStepsCounter = InstrumentationUtils.createNewCounter(Array(metric.name, "failedSteps"))

  override def calculate(): DataFrame = {
    val sqlContext = Session.getSparkSession.sqlContext
    //val spark = Session.getSparkSession

    // val zipsDF = spark.read.json("D:\\Rajan\\OneDrive\\Work\\Projects\\YesBank\\zipcodes.json")
    //zipsDF.show(10,false)
    //println("Total number of zipcodes: " + zipsDF.count())
    //filter all cities whose population > 40K
    //zipsDF.filter(zipsDF.col("pop")> 40000).show(10, false)

    // let's cache
    //zipsDF.cache()
    //zipsDF.createOrReplaceTempView("zips_table")
    //spark.sql("DROP TABLE IF EXISTS zips_hive_table")
    //spark.table("zips_table").write.saveAsTable("zips_hive_table")
    //spark.table("zips_hive_table").cache()
    // make a query to the hive table now
    //val resultsHiveDF = spark.sql("SELECT city, pop, state FROM zips_hive_table WHERE pop > 40000")
    //resultsHiveDF.show(10, false)
    //sqlContext.getAllConfs
    //val configMap:Map[String, String] = Session.getSparkSession.conf.getAll
    var stepResult = sqlContext.emptyDataFrame
    for (step <- metric.steps) {
      try {
        var outputStep = false
        log.info(s"Calculating step ${step.dataFrameName}")
        stepResult = step.actOnDataFrame(sqlContext)
        metric.outputs.map(metricOutput => {
          if (metricOutput.outputConfig.stepName.equalsIgnoreCase(step.dataFrameName)) {
            outputStep = true
          }
        })

        //Caching table logic
        //1.If the status is not mentioned in the metric file or not. This is to support previously written yaml files.
        if(step.getCachingEnableStatus().equalsIgnoreCase("")){
          //2.If the step is not output step then cache it
          if (!outputStep) {
            log.info("No caching status found for step : " + step.dataFrameName)
            log.info("Caching step by default")
            sqlContext.cacheTable(step.dataFrameName)
          }
        }
        //3.If the status is mentioned, then cache according to status
        else{
          //4.If the step is not output step then cache it
          if(!outputStep){
            //5.If enabled, then cache step
            var cachingEnableStatus: Boolean = false
            try{
              cachingEnableStatus = step.getCachingEnableStatus().toBoolean
            }catch {
              case e: Exception => {
                throw ExpressDataUtilityFailedStepException("Invalid status value : " + step.getCachingEnableStatus(), e)
              }
            }
            if(cachingEnableStatus){
              log.info("Caching step : " + step.dataFrameName)
              sqlContext.cacheTable(step.dataFrameName)
            }
          }
        }

        successStepsCounter.inc()
      } catch {
        case ex: Exception => {
          val errorMessage = s"Failed to calculate dataFrame: ${step.dataFrameName} on metric: ${metric.name}"
          failedStepsCounter.inc()
          if (Session.getConfiguration.continueOnFailedStep) {
            log.error(errorMessage, ex)
          } else {
            throw ExpressDataUtilityFailedStepException(errorMessage, ex)
          }
        }
      }
      printStep(stepResult, step.dataFrameName)
    }
    stepResult
  }

  private def printStep(stepResult: DataFrame, stepName: String): Unit = {
    if (Session.getConfiguration.showPreviewLines > 0) {
      log.info(s"Previewing step: ${stepName}")
      try {
        stepResult.printSchema()
        stepResult.show(Session.getConfiguration.showPreviewLines, truncate = false)
      } catch {
        case ex: Exception => {
          log.warn(s"Couldn't print properly step ${stepName}")
        }
      }
    }

  }

}
