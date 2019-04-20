package com.DataUtilityTransform.ExpressDataUtility.output.writers.hbase

import java.io._

import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.google.protobuf.ServiceException
import com.typesafe.config._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class HbaseOutputWriter(props: Map[String, String]) extends MetricOutputWriter {

  case class HbaseOutputConfigurations(hbaseTableConfFile: String, hbaseTaleName: String, rowKeyColumns: String, hbaseZookeeperQuorum: String, hbaseZookeeperPropertyClientPort: String, replaceNullWith: String)

  case class ColumnConfiguration(hbaseColumnName:String, hiveColumnDataType:String, hiveColumnName:String)

  case class ColumnFamilyConfiguration(columnFamiliyName:String, columnFamilyColumnConfiguration:List[ColumnConfiguration])

  @transient val log = LogManager.getLogger(this.getClass)

   val hbaseOutputConfigurations = HbaseOutputConfigurations(
     props("hbaseTableConfFile"),
     props("hbaseTaleName"),
     props("rowKeyColumns"),
     props("hbaseZookeeperQuorum"),
     props("hbaseZookeeperPropertyClientPort"),
     props("replaceNullWith"))

  override def write(dataFrame: DataFrame): Unit = {
    //Check hbase connectivity first
    checkHbaseConnectivity()

    //New hadoop api configuration
    val newAPIJobConfiguration = getHbaseJobConfiguration()

    //Parse hbase configuration file
    val columnFamiliyConfigurations = parseAndGetColumnFamilyConfigurations()

    //Get sequence of hive column names
    val hiveColumnNames = getHiveColumnNames(columnFamiliyConfigurations)

    //Build map of hivecolumn names to its corresponding hbase table column family and column name
    val hiveColumnNameToHbaseColumnMap = buildHiveColumnNameToHbaseColumnMap(columnFamiliyConfigurations)

    //Replacing null values
    log.info("Replacing null values in dataframe with : "+  hbaseOutputConfigurations.replaceNullWith)
    val nullReplacedDataframe = dataFrame.na.fill(hbaseOutputConfigurations.replaceNullWith, hiveColumnNames)

    //Converting dataframe to rdd
    log.info("Converting dataframe to rdd")
    val dataFrameRdd = nullReplacedDataframe.rdd

    //Converting rdd to hbase rdd
    val hbaseRdd = dataFrameRdd.map((row: Row) => {
      val hiveColumnNameToStringValueMap = buildHiveColumnNameToStringValueMap(row, hiveColumnNames)

      val rowKey = buildRowKey(hiveColumnNameToStringValueMap, hbaseOutputConfigurations.rowKeyColumns)

      val put = new Put(Bytes.toBytes(rowKey))

      buildHbaseTableRecord(put, hiveColumnNameToStringValueMap, hiveColumnNameToHbaseColumnMap)

      val hbaseRow = new Tuple2(new ImmutableBytesWritable(), put)
      hbaseRow
    })

    //Insert rdd in hbase
    log.info("Inserting rdd in hbase")
    hbaseRdd.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration)

    log.info("Successfully inserted data in hbase table : " + hbaseOutputConfigurations.hbaseTaleName)
  }

  private def parseAndGetColumnFamilyConfigurations(): List[ColumnFamilyConfiguration] = {
    val config = ConfigFactory.parseFile(new File(hbaseOutputConfigurations.hbaseTableConfFile))
    var columnFamilyConfigurationList = ListBuffer[ColumnFamilyConfiguration]()

    val columnFamiliyConfigurations = config.getObjectList("hbaseTable.hbaseColumnFamilies")
    for(columnFamilyConfigurationValue <- columnFamiliyConfigurations){
      val columnFamilyConfiguration = columnFamilyConfigurationValue.asInstanceOf[ConfigObject].toConfig
      val columnFamilyName = columnFamilyConfiguration.getString("name")

      val columnConfigurations = columnFamilyConfiguration.getList("hbaseCols")
      var columnFamilyColumnList = ListBuffer[ColumnConfiguration]()
      for(columnConfigurationValue <- columnConfigurations){
        val columnConfiguration = columnConfigurationValue.asInstanceOf[ConfigObject].toConfig
        val hbaseColumnName = columnConfiguration.getString("name")
        val hiveColumnDataType = columnConfiguration.getString("type")
        val hiveColumnName = columnConfiguration.getString("hiveColumnName")
        columnFamilyColumnList.add(ColumnConfiguration(hbaseColumnName, hiveColumnDataType, hiveColumnName))
      }
      columnFamilyConfigurationList.add(ColumnFamilyConfiguration(columnFamilyName, columnFamilyColumnList.toList))
    }
    columnFamilyConfigurationList.toList
  }

  private def getHbaseJobConfiguration(): Job ={
    //Create Hbase configuration
    val configuration = getHbaseConfiguration()

    //New hadoop api configuration
    var newAPIJobConfiguration = Job.getInstance(configuration)
    newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseOutputConfigurations.hbaseTaleName)
    newAPIJobConfiguration.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    newAPIJobConfiguration
  }

  private def getHbaseConfiguration(): org.apache.hadoop.conf.Configuration = {
    //Create Hbase configuration
    var configuration = HBaseConfiguration.create();
    configuration.clear()
    configuration.set("hbase.zookeeper.quorum", hbaseOutputConfigurations.hbaseZookeeperQuorum)
    configuration.set("hbase.zookeeper.property.clientPort", hbaseOutputConfigurations.hbaseZookeeperPropertyClientPort)
    configuration.set(TableInputFormat.INPUT_TABLE, hbaseOutputConfigurations.hbaseTaleName)
    configuration
  }

  private def checkHbaseConnectivity(): Unit = {
    //Check if hbase is available or not
    try{
      HBaseAdmin.checkHBaseAvailable(getHbaseConfiguration())
      log.info("Hbase is accessible")
    }catch{
      case e @ (_ : ServiceException | _ : MasterNotRunningException | _ : ZooKeeperConnectionException) => {
        val errorMsg = "HBase is not running";
        throw new Exception(errorMsg, e);
      }
    }
  }

  private def getHiveColumnNames(columnFamilyConfigurations: List[ColumnFamilyConfiguration]): Seq[String] = {
    val hiveColumnNames = ListBuffer[String]()
    for(columnFamilyconfiguration <- columnFamilyConfigurations){
      val columnConfigurations = columnFamilyconfiguration.columnFamilyColumnConfiguration
      for(columnConfiguration <- columnConfigurations){
        hiveColumnNames.add(columnConfiguration.hiveColumnName)
      }
    }
    hiveColumnNames.toSeq
  }

  private def buildHiveColumnNameToStringValueMap(row: Row, hiveColumnNames: Seq[String]): Map[String, String] = {
    var hiveColumnNameToStringValueMap = scala.collection.mutable.Map[String, String]()
    for(hiveColumnName <- hiveColumnNames){
      val hiveColumnValueInString = row.getAs(hiveColumnName).toString
      hiveColumnNameToStringValueMap += (hiveColumnName -> hiveColumnValueInString)
    }
    hiveColumnNameToStringValueMap.toMap
  }

  private def buildRowKey(hiveColumnNameToStringValueMap: Map[String, String], rowKeyColumns: String) : String = {
    val rowKeyColumnNameList = rowKeyColumns.split(",").toList
    var rowKeyColumnValueList = ListBuffer[String]()
    for(rowKeyColumnName <- rowKeyColumnNameList){
      val rowKeyColumnValue = hiveColumnNameToStringValueMap.get(rowKeyColumnName).get
      rowKeyColumnValueList.add(rowKeyColumnValue)
    }
    rowKeyColumnValueList.toList.mkString("_")
  }

  private def buildHiveColumnNameToHbaseColumnMap(columnFamilyConfigurations: List[ColumnFamilyConfiguration]): Map[String, Tuple2[String, String]] = {
    var hiveColumnNameToHbaseColumnMap = scala.collection.mutable.Map[String, Tuple2[String, String]]()
    for(columnFamilyconfiguration <- columnFamilyConfigurations){
      val columnFamilyName = columnFamilyconfiguration.columnFamiliyName
      val columnConfigurations = columnFamilyconfiguration.columnFamilyColumnConfiguration
      for(columnConfiguration <- columnConfigurations){
        val hiveColumnName = columnConfiguration.hiveColumnName
        val hbaseColumnName = columnConfiguration.hbaseColumnName
        hiveColumnNameToHbaseColumnMap += (hiveColumnName -> Tuple2(columnFamilyName, hbaseColumnName))
      }
    }
    hiveColumnNameToHbaseColumnMap.toMap
  }

  private def buildHbaseTableRecord(put: Put, hiveColumnNameToStringValueMap: Map[String, String], hiveColumnNameToHbaseColumnMap:Map[String, Tuple2[String, String]]): Unit ={
    for((hiveColumnName, hiveColumnStringValue) <- hiveColumnNameToStringValueMap){
      val columnFamilyName = hiveColumnNameToHbaseColumnMap.get(hiveColumnName).get._1
      val hbaseColumnName = hiveColumnNameToHbaseColumnMap.get(hiveColumnName).get._2
      put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(hbaseColumnName), Bytes.toBytes(hiveColumnStringValue))
    }
  }
}
