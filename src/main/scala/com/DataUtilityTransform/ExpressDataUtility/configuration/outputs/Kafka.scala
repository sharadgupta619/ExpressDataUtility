package com.DataUtilityTransform.ExpressDataUtility.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Kafka (
              @JsonProperty("kafkaHost") kafkaHost: String,
              @JsonProperty("kafkaPort") kafkaPort: String,
              @JsonProperty("topic") topic: String
            ) {
  require(Option(kafkaHost).isDefined, "Hbase connection: kafkaHost is mandatory")
  require(Option(kafkaPort).isDefined, "Hbase connection: kafkaPort is mandatory")
  require(Option(topic).isDefined, "Hbase connection: topic is mandatory")

}
