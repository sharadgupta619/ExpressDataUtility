package com.DataUtilityTransform.ExpressDataUtility.exceptions

case class ExpressDataUtilityWriteFailedException(private val message: String = "",
                                                  private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
