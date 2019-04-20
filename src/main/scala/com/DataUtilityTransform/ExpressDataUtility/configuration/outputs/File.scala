package com.DataUtilityTransform.ExpressDataUtility.configuration.outputs

import com.fasterxml.jackson.annotation.JsonProperty

case class File(@JsonProperty("dir") dir: String) {
  require(Option(dir).isDefined, "Output file directory: dir is mandatory.")
}
