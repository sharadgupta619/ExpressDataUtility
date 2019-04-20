package com.DataUtilityTransform.ExpressDataUtility.configuration

import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.{Cassandra, ElasticSearch, File, Hbase, Hive, JDBC, Kafka, Redis, Redshift, Segment}
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.{Cassandra, ElasticSearch, File, Hbase, Hive, JDBC, Kafka, Redis, Redshift, Segment}
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs._
import com.fasterxml.jackson.annotation.JsonProperty

case class Output(@JsonProperty("cassandra") cassandra: Option[Cassandra],
                  @JsonProperty("redshift") redshift: Option[Redshift],
                  @JsonProperty("redis") redis: Option[Redis],
                  @JsonProperty("segment") segment: Option[Segment],
                  @JsonProperty("jdbc") jdbc: Option[JDBC],
                  @JsonProperty("jdbcquery") jdbcquery: Option[JDBC],
                  @JsonProperty("file") file: Option[File],
                  @JsonProperty("hive") hive: Option[Hive],
                  @JsonProperty("hbase") hbase: Option[Hbase],
                  @JsonProperty("kafka") kafka: Option[Kafka],
                  @JsonProperty("elasticsearch") elasticsearch: Option[ElasticSearch]
                 ) {}

object Output {
  def apply(): Output = new Output(None ,None, None, None, None, None, None,None,None,None,None)
}
