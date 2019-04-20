package com.DataUtilityTransform.ExpressDataUtility.output

import com.DataUtilityTransform.ExpressDataUtility.metric.config.Output
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.metric.config.Output
import com.DataUtilityTransform.ExpressDataUtility.metric.config.Output

case class MetricOutput(outputConfig: Output, metricName: String) {
  val writer: MetricOutputWriter = MetricOutputWriterFactory.get(outputConfig, metricName)
}
