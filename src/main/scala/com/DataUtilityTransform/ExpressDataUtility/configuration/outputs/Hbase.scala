package com.DataUtilityTransform.ExpressDataUtility.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class Hbase (
                   @JsonProperty("hbaseConfFile") hbaseConfFile: String,
                   @JsonProperty("hbaseTaleName") hbaseTaleName: String,
                   @JsonProperty("rowKey") rowKey: String,
                   @JsonProperty("quorumIp") quorumIp: String,
                   @JsonProperty("hbasePort") hbasePort: String,
                   @JsonProperty("replaceNullWith") replaceNullWith: String
                 ) {
  require(Option(hbaseConfFile).isDefined, "Hbase connection: hbaseConfFile is mandatory")
  require(Option(hbaseTaleName).isDefined, "Hbase connection: hbaseTaleName is mandatory")
  require(Option(rowKey).isDefined, "Hbase connection: rowKey is mandatory")
  require(Option(quorumIp).isDefined, "Hbase connection: quorumIp is mandatory")
  require(Option(hbasePort).isDefined, "Hbase connection: hbasePort is mandatory")
  require(Option(replaceNullWith).isDefined, "Hbase connection: replaceNullWith is mandatory")

}
