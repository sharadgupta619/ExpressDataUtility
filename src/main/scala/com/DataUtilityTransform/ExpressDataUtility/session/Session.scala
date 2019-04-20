package com.DataUtilityTransform.ExpressDataUtility.session

import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, DateRange, Input, Output}
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtility.input.InputTableReader
import com.DataUtilityTransform.ExpressDataUtility.output.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redis.RedisOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, DateRange, Input, Output}
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtility.input.InputTableReader
import com.DataUtilityTransform.ExpressDataUtility.output.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redis.RedisOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.configuration.{Configuration, DateRange, Input, Output}
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtility.input.InputTableReader
import com.DataUtilityTransform.ExpressDataUtility.output.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SparkSession

object Session {
  val log = LogManager.getLogger(this.getClass)

  private var configuration: Option[Configuration] = None
  private var spark: Option[SparkSession] = None

  def init(config: Configuration) {
    spark = Some(createSparkSession(config.appName, config.output))
    setSparkLogLevel(config.logLevel)
    registerVariables(config.variables)
    //registerDataframes(config.inputs, config.dateRange)
    configuration = Some(config)
  }

  private def setSparkLogLevel(logLevel: String) {
    //TODO Remove the usage of null in this class
    // scalastyle:off null
    if (logLevel != null) {
      getSparkSession.sparkContext.setLogLevel(logLevel)
    }
    // scalastyle:on null
  }

  def getConfiguration: Configuration = {
    if (configuration.isDefined) {
      configuration.get
    }
    else {
      throw ExpressDataUtilityException(s"Session Configuration Must Be Set")
    }
  }


  def getSparkSession: SparkSession = {
    if (spark.isDefined) {
      spark.get
    }
    else {
      throw ExpressDataUtilityException(s"Session Configuration Must Be Set")
    }
  }

  private def registerVariables(variables: Map[String, String]): Unit = {
    variables.foreach({ case (key, value) => {
      getSparkSession.sql(s"set $key=$value")
    }
    })
  }

  def registerDataframes(inputs: Seq[Input], dateRange: Map[String, DateRange]): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(input => {
        log.info(s"Registering ${input.name} table")
        val dateRangeOption: Option[DateRange] = dateRange.get(input.name)
        val tablePaths: Seq[String] = if (dateRangeOption.isEmpty) Seq(input.path) else dateRangeOption.get.replace(input.path)
        val reader = InputTableReader(tablePaths)
        val df = reader.read(tablePaths)
        df.createOrReplaceTempView(input.name)
      })
    }
  }

  private def createSparkSession(appName: String, output: Output): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName)
    if (output.cassandra.isDefined) {
      CassandraOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.cassandra.get)
    }
    if (output.redis.isDefined) {
      RedisOutputWriter.addConfToSparkSession(sparkSessionBuilder, output.redis.get)
    }
    //val session = sparkSessionBuilder.config("hive.metastore.warehouse.dir", "D:\\Rajan\\OneDrive\\Work\\Projects\\YesBank\\ExpressDataUtility\\spark-warehouse").enableHiveSupport().getOrCreate()
    val session = sparkSessionBuilder.enableHiveSupport().getOrCreate()
    UserMetricsSystem.initialize(session.sparkContext, "ExpressDataUtility")
    session
  }
}
