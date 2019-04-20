package com.DataUtilityTransform.ExpressDataUtility.output.writers.hbase

import java.io._

import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.google.protobuf.ServiceException
import com.typesafe.config._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class BulkHbaseOutputWriter(props: Map[String, String]) extends MetricOutputWriter {

  case class HbaseOutputConfigurations(hbaseTableConfFile: String, hbaseTaleName: String, rowKeyColumns: String, hbaseZookeeperQuorum: String, hbaseZookeeperPropertyClientPort: String,
                                       replaceNullWith: String, tempHfilePath:String)

  case class ColumnConfiguration(hbaseColumnName:String, hiveColumnDataType:String, hiveColumnName:String)

  case class ColumnFamilyConfiguration(columnFamiliyName:String, columnFamilyColumnConfiguration:List[ColumnConfiguration])

  @transient val log = LogManager.getLogger(this.getClass)

  val hbaseOutputConfigurations = HbaseOutputConfigurations(
    props("hbaseTableConfFile"),
    props("hbaseTaleName"),
    props("rowKeyColumns"),
    props("hbaseZookeeperQuorum"),
    props("hbaseZookeeperPropertyClientPort"),
    props("replaceNullWith"),
    props("tempHfilePath"))

  override def write(dataFrame: DataFrame): Unit = {
    //Build hbase configuration
    val hBaseConfiguration:org.apache.hadoop.conf.Configuration = getHbaseConfiguration()

    //Check hbase connectivity first
    checkHbaseConnectivity(hBaseConfiguration)

    //Build job configuration object
    val jobConfiguration:Job = buildJobConfiguration(hBaseConfiguration)

    //Get hbase table object
    val hbaseTableObject:HTable = buildHbaseTableObject(jobConfiguration, hBaseConfiguration)

    //Generate rdd here
    val outputDataFrameRdd:RDD[(ImmutableBytesWritable, KeyValue)] = buildHbaseCompatibleDataframeRdd(dataFrame)

    //Write rdd to hfile
    writeRddToHfile(jobConfiguration, hBaseConfiguration, hbaseTableObject, outputDataFrameRdd)

    //Upload hfile to hbase
    uploadHfileToHbase(hBaseConfiguration, hbaseTableObject)
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

  private def checkHbaseConnectivity(configuration:org.apache.hadoop.conf.Configuration): Unit = {
    //Check if hbase is available or not
    try{
      HBaseAdmin.checkHBaseAvailable(configuration)
      log.info("Hbase is accessible")
    }catch{
      case e @ (_ : ServiceException | _ : MasterNotRunningException | _ : ZooKeeperConnectionException) => {
        val errorMsg = "HBase is not running";
        throw new Exception(errorMsg, e);
      }
    }
  }

  private def buildJobConfiguration(hbaseConfig:org.apache.hadoop.conf.Configuration): Job = {
    val jobName:String = hbaseOutputConfigurations.hbaseTaleName + " bulk upload"
    val job:Job = Job.getInstance(hbaseConfig, jobName)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job
  }

  private def buildHbaseTableObject(job:Job, hbaseConfig:org.apache.hadoop.conf.Configuration): HTable = {
    val hbaseTableObject:HTable = new HTable(hbaseConfig, hbaseOutputConfigurations.hbaseTaleName)
    hbaseTableObject
  }

  private def writeRddToHfile(job:Job, hbaseConfig:org.apache.hadoop.conf.Configuration, hbaseTableObject:HTable, outputRdd:RDD[(ImmutableBytesWritable, KeyValue)]): Unit = {
    val tableName = TableName.valueOf(hbaseOutputConfigurations.hbaseTaleName)
    val connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(tableName)
    val tableDescriptor:HTableDescriptor = table.getTableDescriptor
    val regionLocator:RegionLocator = connection.getRegionLocator(tableName)
    HFileOutputFormat2.configureIncrementalLoad(job, tableDescriptor, regionLocator)
    outputRdd.saveAsNewAPIHadoopFile(hbaseOutputConfigurations.tempHfilePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], hbaseConfig)
    connection.close()
  }

  private def uploadHfileToHbase(hbaseConfig:org.apache.hadoop.conf.Configuration, hbaseTableObject:HTable):Unit = {
    val bulkLoader = new LoadIncrementalHFiles(hbaseConfig)
    bulkLoader.doBulkLoad(new Path(hbaseOutputConfigurations.tempHfilePath), hbaseTableObject)
  }

  private def buildHbaseCompatibleDataframeRdd(dataFrame: DataFrame): RDD[(ImmutableBytesWritable, KeyValue)] = {
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
    val rowKeyAndRowColsRdd = dataFrameRdd.map((row: Row) => {
      val hiveColumnNameToStringValueMap = buildHiveColumnNameToStringValueMap(row, hiveColumnNames)
      val rowKey = buildRowKey(hiveColumnNameToStringValueMap, hbaseOutputConfigurations.rowKeyColumns)
      val rowColumns = buildHbaseTableRow(hiveColumnNameToStringValueMap, hiveColumnNameToHbaseColumnMap)
      val tuple = new Tuple2(rowKey, rowColumns.toList)
      tuple
    })

    val flattenRowKeyAndRowColsRdd = rowKeyAndRowColsRdd.flatMap(element => {
      val rowKey = element._1
      val rowColumns = element._2
      for(rowColumn <- rowColumns) yield {
       val columnFamilyName = rowColumn._1
        val columnName = rowColumn._2
        val columnValue = rowColumn._3
        val tuple = new Tuple3(columnFamilyName, columnName, columnValue)
        (rowKey, tuple)
      }
    })

    val hbaseCompatibleRdd = flattenRowKeyAndRowColsRdd.map(element => {
      val rowKey = element._1
      val immutableRowKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
      val columnFamilyName = element._2._1
      val columnName = element._2._2
      val columnValue = element._2._3

      val keyValue = new KeyValue(rowKey.getBytes(), columnFamilyName.getBytes(), columnName.getBytes(), columnValue.getBytes())
      (immutableRowKey, keyValue)
    })

    hbaseCompatibleRdd
  }

  private def buildHbaseTableRow(hiveColumnNameToStringValueMap: Map[String, String], hiveColumnNameToHbaseColumnMap:Map[String, Tuple2[String, String]]): List[Tuple3[String,String,String]] = {
    var hbaseTableRow = ListBuffer[Tuple3[String,String,String]]()
    for((hiveColumnName, hiveColumnStringValue) <- hiveColumnNameToStringValueMap){
      val columnFamilyName = hiveColumnNameToHbaseColumnMap.get(hiveColumnName).get._1
      val hbaseColumnName = hiveColumnNameToHbaseColumnMap.get(hiveColumnName).get._2
      hbaseTableRow.add(columnFamilyName, hbaseColumnName, hiveColumnStringValue)
    }
    hbaseTableRow.toList
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
}
