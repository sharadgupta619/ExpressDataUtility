package com.DataUtilityTransform.ExpressDataUtility.metric

import java.io.File

import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.metric.step.{Sql, StepAction}
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutput
import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.metric.step.{Sql, StepAction}
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutput
import com.DataUtilityTransform.ExpressDataUtility.metric.config.MetricConfig
import com.DataUtilityTransform.ExpressDataUtility.metric.step.{Sql, StepAction}
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutput

class Metric(metricConfig: MetricConfig, metricDir: File, metricName: String) {
  val name: String = metricName
  val steps: List[StepAction] = metricConfig.steps.map(stepConfig => Sql(stepConfig.getSqlQuery(metricDir), stepConfig.stepName, stepConfig.getPartitions(), stepConfig.getCachingEnableStatus()))
  val outputs: List[MetricOutput] = Option(metricConfig.output) match {
    case None => List()
    case Some(metricConfig.output) => metricConfig.output.map(MetricOutput(_, name))
  }
}


