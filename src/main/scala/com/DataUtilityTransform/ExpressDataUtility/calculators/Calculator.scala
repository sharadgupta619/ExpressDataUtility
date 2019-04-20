package com.DataUtilityTransform.ExpressDataUtility.calculators

import org.apache.spark.sql.DataFrame

trait Calculator {
  def calculate(): DataFrame
}
