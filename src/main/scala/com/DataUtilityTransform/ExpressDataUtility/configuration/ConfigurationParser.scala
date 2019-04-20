package com.DataUtilityTransform.ExpressDataUtility.configuration

import java.io.FileReader

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import CLIConfigurationParser.ConfigFileName
import com.DataUtilityTransform.ExpressDataUtility.exceptions.{ExpressDataUtilityException, ExpressDataUtilityInvalidMetricFileException}
import com.DataUtilityTransform.ExpressDataUtility.utils.FileUtils
import com.DataUtilityTransform.ExpressDataUtility.exceptions.{ExpressDataUtilityException, ExpressDataUtilityInvalidMetricFileException}
import com.DataUtilityTransform.ExpressDataUtility.exceptions.{ExpressDataUtilityException, ExpressDataUtilityInvalidMetricFileException}
import org.apache.log4j.{LogManager, Logger}

object ConfigurationParser {
  val log: Logger = LogManager.getLogger(this.getClass)

  def parse(args: Array[String]): ConfigurationFile = {
    log.info("Starting ExpressDataUtility - Parsing configuration")

    CLIConfigurationParser.parser.parse(args, ConfigFileName()) match {
      case Some(arguments) =>
        parseConfigurationFile(arguments.filename)
      case None => throw new ExpressDataUtilityException("Failed to parse config file")
    }
  }

  def parseConfigurationFile(fileName: String): ConfigurationFile = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new FileReader(fileName), classOf[ConfigurationFile])
      }
      case None => throw ExpressDataUtilityInvalidMetricFileException(s"Unknown extension for file $fileName")
    }
  }
}
