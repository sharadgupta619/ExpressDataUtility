package com.DataUtilityTransform.ExpressDataUtility.output.writers.jdbc

import java.sql.DriverManager
import java.util.Properties

import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.JDBC
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}


class JDBCOutputWriter(props: Map[String, String]) extends MetricOutputWriter {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  case class JDBCOutputProperties(databaseType: String, dbUrl: String, userName: String, password: String, driverName: String, tableName: String, saveMode: String, batchSize: String)

  //Check if batch size is given. If not then use default as 1000
  var batchSize = "1000"
  if(props.contains("batchSize")){
    batchSize = props("batchSize")
  }

  val jdbcOutputProps = JDBCOutputProperties(props("databaseType"), props("dbUrl"), props("userName"), props("password"), props("driverName"), props("tableName"), props("saveMode"), batchSize)

  override def write(dataFrame: DataFrame): Unit = {
    //Check if driver class is available in class path
    Class.forName(jdbcOutputProps.driverName)

    //Create connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", jdbcOutputProps.userName)
    connectionProperties.put("password", jdbcOutputProps.password)
    connectionProperties.put("driver", jdbcOutputProps.driverName)
    connectionProperties.setProperty("AutoCommit", "true")

    //Check database connectivity
    val connection = DriverManager.getConnection(jdbcOutputProps.dbUrl, jdbcOutputProps.userName, jdbcOutputProps.password)
    connection.isClosed()

    log.info("Using batch size : " + jdbcOutputProps.batchSize)

    //Write data frame to database
    dataFrame
      .write
      .format("jdbc")
      .mode(jdbcOutputProps.saveMode)
      .option("batchsize", jdbcOutputProps.batchSize.toLong)
      .jdbc(jdbcOutputProps.dbUrl, jdbcOutputProps.tableName, connectionProperties)

    /*dataFrame.write
      .format("jdbc")
      .mode(jdbcOutputProps.saveMode)
      .option("driver", jdbcOutputProps.driverName)
      .option("url", jdbcOutputProps.dbUrl)
      .option("dbtable", jdbcOutputProps.tableName)
      .option("user", jdbcOutputProps.userName)
      .option("password", jdbcOutputProps.password)
      .option("batchsize", jdbcOutputProps.batchSize.toLong)
      .save()*/

    /*jdbcConf match {
      case Some(jdbcConf) =>
        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcConf.user)
        connectionProperties.put("password", jdbcConf.password)

        var df = dataFrame
        val writer = df.write.format(jdbcConf.driver)
          .mode(jdbcOutputProps.saveMode)
          .jdbc(jdbcConf.connectionUrl, jdbcOutputProps.dbTable, connectionProperties)
      case None =>*/
    }
}
