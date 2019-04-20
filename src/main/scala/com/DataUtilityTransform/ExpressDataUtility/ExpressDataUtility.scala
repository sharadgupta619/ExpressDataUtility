package com.DataUtilityTransform.ExpressDataUtility

import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, ConfigurationParser}
import com.DataUtilityTransform.ExpressDataUtility.metric.MetricSet
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, ConfigurationParser}
import com.DataUtilityTransform.ExpressDataUtility.metric.MetricSet
import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, ConfigurationParser}
import com.DataUtilityTransform.ExpressDataUtility.metric.MetricSet
import org.apache.log4j.LogManager

/**
  * ExpressDataUtility - runs Spark SQL queries on various data sources and exports the results
  */
object ExpressDataUtility extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting ExpressDataUtility - Parsing configuration")
  printf("args: %s",args)
  val config: Configuration = ConfigurationParser.parse(args)
  Session.init(config)
  runMetrics

  def runMetrics(): Unit = {
    Session.getConfiguration.metrics.foreach(metricSetPath => {
      val metricSet = new MetricSet(metricSetPath)
      metricSet.run()
    })
  }

}
