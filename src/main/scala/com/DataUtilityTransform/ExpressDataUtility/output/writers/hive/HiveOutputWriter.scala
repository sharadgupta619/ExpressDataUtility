package com.DataUtilityTransform.ExpressDataUtility.output.writers.hive

import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.Hive
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.Hive
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.Hive
import org.apache.log4j.LogManager

class HiveOutputWriter(props: Map[String, String], hiveConf: Option[Hive]) extends MetricOutputWriter {

  case class HiveOutputProperties(schemaName: String, tableName: String, tableDefination: String, saveMode: String, execProperties: String, intermediate: Boolean)

  @transient val log = LogManager.getLogger(this.getClass)
  var schemaName = "default"
  var intermediate = false
  if (props.contains("schemaName")) {
    schemaName = props("schemaName")
                                                                                                                        }
  if (props.contains("intermediate")) {
    intermediate = props("intermediate").toBoolean
  }
  var hiveOptions = HiveOutputProperties(schemaName, props("tableName"), null, props("saveMode"), props("execProperties"), intermediate)

  override def write(dataFrame: DataFrame): Unit = {
    val spark = Session.getSparkSession

    //Check, configure and execute if table defination exists.
    if(props.contains("tableDefination")){
      hiveOptions = HiveOutputProperties(schemaName, props("tableName"), props("tableDefination"), props("saveMode"), props("execProperties"), intermediate)
      log.info("Executing table defination : " + hiveOptions.tableDefination)
      spark.sql(hiveOptions.tableDefination)
    }else{
      log.info("Table defination does not exists")
    }

    //spark.sql("use " + hiveOptions.schemaName)
    //var df = dataFrame
    val execPropList = hiveOptions.execProperties.split(";")
    execPropList.map(prop => {
      log.info(s"Setting Hive property: ${prop}")
      spark.sql("SET " + prop)
    })
    //df.write.mode(hiveOptions.saveMode)saveAsTable(hiveOptions.tableName)
    dataFrame.write.mode(hiveOptions.saveMode).insertInto(hiveOptions.schemaName + "." + hiveOptions.tableName)
    //log.info(s"No. of records inserted in the table ${hiveOptions.tableName} = "+dataFrame.count())
    log.info(s"**********Records successfully inserted into table : ${hiveOptions.tableName}.**********")
    dataFrame.unpersist()
    //log.info(s"Get count of table:  ${hiveOptions.tableName} ")
    //spark.sql("SELECT count(*) as Records_Count from " + hiveOptions.schemaName+"."+hiveOptions.tableName).show()
  }

  def truncate(dataFrame: DataFrame): Unit = {
    log.info(s"Inside Truncate Function")
    val spark = Session.getSparkSession
    if (hiveOptions.intermediate) {
      spark.sql("TRUNCATE TABLE " + hiveOptions.schemaName + "." + hiveOptions.tableName)
      log.info(s"Truncated table : ${hiveOptions.schemaName}.${hiveOptions.tableName}")
    }
  }
}
