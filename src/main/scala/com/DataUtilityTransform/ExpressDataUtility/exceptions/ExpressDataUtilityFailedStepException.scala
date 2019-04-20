package com.DataUtilityTransform.ExpressDataUtility.exceptions

case class ExpressDataUtilityFailedStepException(private val message: String = "",
                                                 private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
