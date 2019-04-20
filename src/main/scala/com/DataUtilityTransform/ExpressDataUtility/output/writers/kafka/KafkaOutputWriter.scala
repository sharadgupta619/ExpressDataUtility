package com.DataUtilityTransform.ExpressDataUtility.output.writers.kafka

import java.io.{ByteArrayOutputStream, File}

import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

class KafkaOutputWriter(props: Map[String, String]) extends MetricOutputWriter {
  case class KafkaOutputProperties(bootstrapServers: String, topic: String, recordDelimiter: String, recordFormat: String, recordKeyColumns:String, recordSchemaFile: String)

  var recordSchemaFile: String = null
  if(props.contains("recordSchemaFile")){
    recordSchemaFile = props("recordSchemaFile")
  }

  @transient val log = LogManager.getLogger(this.getClass)

  var recordKeyColumnsProperty:String = null
  if(props.contains("recordKeyColumns")){
    recordKeyColumnsProperty = props("recordKeyColumns")
  }else{
    log.info("No recordKey columns specified.")
  }

  var recordDelimiterProperty: String = ","
  if(props.contains("recordDelimiter")){
    recordDelimiterProperty = props("recordDelimiter")
  }else{
    log.info("No record delimiter specified. Using default in case of text output format => ,")
  }

  val kafkaOutputProps = KafkaOutputProperties(props("bootstrapServers"),props("topic"), recordDelimiterProperty, props("recordFormat"), recordKeyColumnsProperty, recordSchemaFile)

  override def write(dataFrame: DataFrame): Unit = {

    //Hive column names from dataframe
    val hiveColumnNames = dataFrame.columns.toList

    //Converting dataframe to rdd
    val dataFrameRdd = dataFrame.rdd

    //Get schema of dataframe
    val dataFrameSchema = struct(dataFrame.columns.map(column):_*)

    var recordKeyColumnList: List[String] = null
    if(kafkaOutputProps.recordKeyColumns != null){
      //RecordKeyColumns
      recordKeyColumnList = kafkaOutputProps.recordKeyColumns.split(",").toList
      insertDataFrameInKafkaWithKey(dataFrameRdd, hiveColumnNames, recordKeyColumnList)
    }else {
      insertDataFrameInKafkaWithoutKey(dataFrameRdd, hiveColumnNames)
    }
  }

  private def buildRecordKey(row: Row, recordKeyColumnList: List[String]): String = {
    var recordKeyColumnValueList = ListBuffer[String]()
    for(rowKeyColumnName <- recordKeyColumnList){
      val recordKeyColumnValue = row.getAs(rowKeyColumnName).toString
      recordKeyColumnValueList.add(recordKeyColumnValue)
    }
    recordKeyColumnValueList.mkString("_")
  }

  private def buildDelimiterSeparatedRddRow(row: Row, hiveColumnNames:List[String]): String = {
    var columnValuesList = ListBuffer[String]()
    if(kafkaOutputProps.recordFormat.equalsIgnoreCase("text")){
      for(hiveColumnName <- hiveColumnNames){
        val colValue = row.getAs(hiveColumnName).toString
        columnValuesList.add(colValue)
      }
      return columnValuesList.toList.mkString(kafkaOutputProps.recordDelimiter)
    }else if(kafkaOutputProps.recordFormat.equalsIgnoreCase("json")){
      val rowValuesMap = row.getValuesMap(row.schema.fieldNames)
      JSONObject(rowValuesMap).toString()
    }else if(kafkaOutputProps.recordFormat.equalsIgnoreCase("avro")){
      val avroRecord = getAvroRecordFromRow(row)
      avroRecord.toString
    }else{
      null
    }
  }

  private def insertDataFrameInKafkaWithKey(dataFrameRdd:RDD[Row], hiveColumnNames: List[String], recordKeyColumnList:List[String]): Unit = {
    val spark = Session.getSparkSession

    //Converting rdd to kafka rdd
    val kafkaRdd = dataFrameRdd.map((row: Row) => {
      val delimiterSeparatedRddRow = buildDelimiterSeparatedRddRow(row, hiveColumnNames)
      val recordKey = buildRecordKey(row, recordKeyColumnList)
      new Tuple2(recordKey, delimiterSeparatedRddRow)
    })

    //Converting kafkaRdd to kafkaDataframe
    val kafkaDataframe = spark.createDataFrame(kafkaRdd).toDF("key", "value")

    //Write kafka dataframe to kafka
    val kafkaWriteResult = kafkaDataframe.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaOutputProps.bootstrapServers)
      .option("topic", kafkaOutputProps.topic)
      .save()
  }

  private def insertDataFrameInKafkaWithoutKey(dataFrameRdd:RDD[Row], hiveColumnNames: List[String]): Unit = {
    val spark = Session.getSparkSession

    //Converting rdd to kafka rdd
    val kafkaRdd = dataFrameRdd.map((row: Row) => {
      val delimiterSeparatedRddRow = buildDelimiterSeparatedRddRow(row, hiveColumnNames)
      new Tuple1(delimiterSeparatedRddRow)
    })

    //Converting kafkaRdd to kafkaDataframe
    val kafkaDataframe = spark.createDataFrame(kafkaRdd).toDF("value")

    //Write kafka dataframe to kafka
    val kafkaWriteResult = kafkaDataframe.selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaOutputProps.bootstrapServers)
      .option("topic", kafkaOutputProps.topic)
      .save()
  }

  private def getAvroRecordFromRow(row: Row): Array[Byte] = {
    var schema: Schema = null
    if(kafkaOutputProps.recordSchemaFile != null) {
      schema = new Parser().parse(new File(kafkaOutputProps.recordSchemaFile))
    }

    val gr: GenericRecord = new GenericData.Record(schema)
    row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(gr, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }
}
