package com.DataUtilityTransform.ExpressDataUtility.output

import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtility.metric.config.Output
import com.DataUtilityTransform.ExpressDataUtility.output.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.csv.CSVOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.elasticsearch.ElasticsearchOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hbase.HbaseOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.instrumentation.InstrumentationOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter}
import com.DataUtilityTransform.ExpressDataUtility.output.writers.json.JSONOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.kafka.KafkaOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.parquet.ParquetOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redis.RedisOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redshift.RedshiftOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.segment.SegmentOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.exceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtility.metric.config.Output
import com.DataUtilityTransform.ExpressDataUtility.output.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.csv.CSVOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.elasticsearch.ElasticsearchOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hbase.HbaseOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.instrumentation.InstrumentationOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter}
import com.DataUtilityTransform.ExpressDataUtility.output.writers.json.JSONOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.kafka.KafkaOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.parquet.ParquetOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redis.RedisOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.redshift.RedshiftOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.output.writers.segment.SegmentOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtilityexceptions.ExpressDataUtilityException
import com.DataUtilityTransform.ExpressDataUtilitymetric.config.Output
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.cassandra.CassandraOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.csv.CSVOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.elasticsearch.ElasticsearchOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.hbase.HbaseOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.hive.HiveOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.instrumentation.InstrumentationOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.jdbc.{JDBCOutputWriter, JDBCQueryWriter}
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.json.JSONOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.kafka.KafkaOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.parquet.ParquetOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.redis.RedisOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.redshift.RedshiftOutputWriter
import com.DataUtilityTransform.ExpressDataUtilityoutput.writers.segment.SegmentOutputWriter
import com.DataUtilityTransform.ExpressDataUtilitysession.Session

object MetricOutputWriterFactory {
  def get(outputConfig: Output, metricName: String): MetricOutputWriter = {
    val output = Session.getConfiguration.output
    val metricOutputOptions = outputConfig.outputOptions
    val outputType = OutputType.withName(outputConfig.outputType)
    val metricOutputWriter = outputType match {
      //TODO: move casting into the writer class
      case OutputType.Cassandra => new CassandraOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]]) //TODO add here cassandra from session
      case OutputType.Redshift => new RedshiftOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.redshift)
      case OutputType.Redis => new RedisOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]]) //TODO add here redis from session
      case OutputType.Segment => new SegmentOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.segment)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.file)
      case OutputType.Instrumentation => new InstrumentationOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], outputConfig.stepName, metricName)
      case OutputType.JDBC => new JDBCOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]])
      case OutputType.JDBCQuery => new JDBCQueryWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.jdbcquery)
      case OutputType.Hive => new HiveOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]], output.hive)
      case OutputType.Hbase  => new HbaseOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]])
      case OutputType.HbaseBulk  => new HbaseOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]])
      case OutputType.Kafka  => new KafkaOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]])
      case OutputType.ElasticSearch  => new ElasticsearchOutputWriter(metricOutputOptions
        .asInstanceOf[Map[String, String]])
      case _ => throw new ExpressDataUtilityException(s"Not Supported Writer $outputType")
    }
    metricOutputWriter.validateMandatoryArguments(metricOutputOptions.asInstanceOf[Map[String, String]])
    metricOutputWriter
  }

  def getHiveWriter(outputConfig: Output, metricName: String): HiveOutputWriter = {
    val output = Session.getConfiguration.output
    val metricOutputOptions = outputConfig.outputOptions
    val outputType = OutputType.withName(outputConfig.outputType)
    val hiveWriter = new HiveOutputWriter(metricOutputOptions
      .asInstanceOf[Map[String, String]], output.hive)
    hiveWriter
  }
}

object OutputType extends Enumeration {
  val Parquet: OutputType.Value = Value("Parquet")
  val Cassandra: OutputType.Value = Value("Cassandra")
  val CSV: OutputType.Value = Value("CSV")
  val JSON: OutputType.Value = Value("JSON")
  val Redshift: OutputType.Value = Value("Redshift")
  val Redis: OutputType.Value = Value("Redis")
  val Segment: OutputType.Value = Value("Segment")
  val Instrumentation: OutputType.Value = Value("Instrumentation")
  val JDBC: OutputType.Value = Value("JDBC")
  val JDBCQuery: OutputType.Value = Value("JDBCQuery")
  val Hive: OutputType.Value = Value("Hive")
  val Hbase: OutputType.Value = Value("Hbase")
  val HbaseBulk: OutputType.Value = Value("HbaseBulk")
  val Kafka: OutputType.Value = Value("Kafka")
  val ElasticSearch: OutputType.Value = Value("ElasticSearch")
}
