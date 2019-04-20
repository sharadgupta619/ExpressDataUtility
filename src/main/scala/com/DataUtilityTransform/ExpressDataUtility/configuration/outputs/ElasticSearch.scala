package com.DataUtilityTransform.ExpressDataUtility.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class ElasticSearch (
                      @JsonProperty("documentName") documentName: String
                    ) {
  require(Option(documentName).isDefined, "Elastic Search connection: documentName is mandatory")

}
