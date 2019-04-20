package com.DataUtilityTransform.ExpressDataUtility.configuration

import java.nio.file.{Files, Paths}

import scopt.OptionParser

object CLIConfigurationParser {

  val parser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("ExpressDataUtility") {
    head("ExpressDataUtility", "1.0")
    opt[String]('c', "config")
      .text("The YAML file that defines the ExpressDataUtility arguments")
      .action((x, c) => c.copy(filename = x))
      .validate(x => {
        if (Files.exists(Paths.get(x))) {
          success
        }
        else {
          failure("Supplied YAML file not found")
        }
      }).required()
    help("help") text "use command line arguments to specify the YAML configuration file path"
  }

  case class ConfigFileName(filename: String = "")

}
