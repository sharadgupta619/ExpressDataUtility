package com.DataUtilityTransform.ExpressDataUtility.output.writers.csv

import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.File
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.File
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.configuration.outputs.File
import com.DataUtilityTransform.ExpressDataUtility.output.MetricOutputWriter
import com.DataUtilityTransform.ExpressDataUtility.session.Session
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class CSVOutputWriter(props: Map[String, String], outputFile: Option[File]) extends MetricOutputWriter {

  case class CSVOutputProperties(saveMode: String, path: String, fileName: String, coalesce: Boolean, csvOptions: Map[String,String])

  val log = LogManager.getLogger(this.getClass)

  //Create file system objects
  val fileSystemConfig = new Configuration()
  val localFileSystem = FileSystem.getLocal(fileSystemConfig)
  val hdfsFileSystem = FileSystem.get(fileSystemConfig)

  //Check of coalesce option is given or not. If not then set it to true by default
  val coalesce = props.getOrElse("coalesce", true).asInstanceOf[Boolean]

  //Check if filename option is given or not.
  var fileName: String = null
  if(props.contains("fileName")){
    fileName = props("fileName")
  }

  //Combine default and yaml file csv options to get final csv options
  val finalCsvOptions = getFinalCsvOptions()

  //Create csv output properties object
  var csvOutputProps = CSVOutputProperties(props("saveMode"), props("path"), fileName, coalesce, finalCsvOptions)

  override def write(dataFrame: DataFrame): Unit = {
    outputFile match {
      case Some(outputFile) =>
        //Check if coalesce is required or not
        val df = if (csvOutputProps.coalesce) dataFrame.coalesce(1) else dataFrame

        //Build output directory path
        val outputDirectoryPath = outputFile.dir + "/" + csvOutputProps.path

        //Check if output directory is local or not
        if(isLocalDirectoryOutput(outputDirectoryPath)){
          //Temp hdfs path to which output will be written first. From thies temp file output will be written to given local file.
          val tempHdfsDirPath = ".sparkStaging/" + System.currentTimeMillis.toString + "_tmp"
          //Delete temp hdfs directory if exists
          checkAndDeleteHdfsDir(tempHdfsDirPath)

          log.info("Writing output to local directory : " + outputDirectoryPath)
          var outputPathFileName = outputDirectoryPath + java.io.File.separator + "data_" + System.currentTimeMillis.toString + ".csv"
          if(csvOutputProps.fileName != null){
            outputPathFileName = outputDirectoryPath + java.io.File.separator + csvOutputProps.fileName
          }
          log.info("Writing data to local file : " + outputPathFileName)

          //If the mode is append
          if(csvOutputProps.saveMode.equalsIgnoreCase("append")){
            //If your local output file is present, it means this is not first run.
            if(isLocalFileExists(outputPathFileName)){
              var prevDataFrame: DataFrame = null
              //If header is enabled, then we are assuming that header was enabled for first run also
              if(isHeaderEnabled(csvOutputProps.csvOptions)){
                log.info("Header option is enabled.")
                //Get dataframe from already existing output file with header skipped
                prevDataFrame = getPrevDataFrameWithFirstLineSkipFromLocalFile(outputPathFileName, df.schema)
              }
              //If header is disabled, then we are assuming that header was disabled for first run also.
              else{
                log.info("Header option is disabled.")
                //Get dataframe from already existing output file without skipping first line
                prevDataFrame = getPrevDataFrameWithoutFirstLineSkipFromLocalFile(outputPathFileName, df.schema)
              }
              //Union this previous data's dataframe with actual dataframe
              val unionDataframe = prevDataFrame.union(df)
              //Write this unioned dataframe now. Note that since header is true, it will be written in output file.
              val csvOptionsWithFalseHeader = getCsvOptionsWithGivenHeaderValue("false")
              logCsvOptions(csvOptionsWithFalseHeader)
              //This one will be written without header
              unionDataframe.write.mode(csvOutputProps.saveMode).options(csvOptionsWithFalseHeader).csv(tempHdfsDirPath)
              //So if our header is eanbled then we have to do one extra step
              if(isHeaderEnabled(csvOutputProps.csvOptions)){
                val temp_outputPathFileName2 = outputPathFileName + "_temp2"
                checkAndDeleteLocalFile(temp_outputPathFileName2)
                FileUtil.copyMerge(hdfsFileSystem, new Path(tempHdfsDirPath), localFileSystem, new Path(temp_outputPathFileName2), true , fileSystemConfig, null)
                checkAndDeleteHdfsDir(tempHdfsDirPath)
                val dataFrameFromTempFile = Session.getSparkSession.read.format("csv").schema(df.schema).options(csvOptionsWithFalseHeader).load(temp_outputPathFileName2)
                dataFrameFromTempFile.write.mode(csvOutputProps.saveMode).options(csvOutputProps.csvOptions).csv(tempHdfsDirPath)
              }
            }
            //If local output file is not present. Then it means it is first run
            else{
              //Write dataframe as it is
              logCsvOptions(csvOutputProps.csvOptions)
              df.write.mode(csvOutputProps.saveMode).options(csvOutputProps.csvOptions).csv(tempHdfsDirPath)
            }
          }
          //If the mode is overwrite
          else{
            logCsvOptions(csvOutputProps.csvOptions)
            df.write.mode(csvOutputProps.saveMode).options(csvOutputProps.csvOptions).csv(tempHdfsDirPath)
          }
          //Create local temp output file path
          val temp_outputPathFileName = outputPathFileName + "_temp"
          //Delete that local temp file first if it exists
          checkAndDeleteLocalFile(temp_outputPathFileName)
          //Write data to local temp output file
          FileUtil.copyMerge(hdfsFileSystem, new Path(tempHdfsDirPath), localFileSystem, new Path(temp_outputPathFileName), true , fileSystemConfig, null)
          //Delete temp hdfs path
          checkAndDeleteHdfsDir(tempHdfsDirPath)
          //Delete output file if exists
          checkAndDeleteLocalFile(outputPathFileName)
          //Rename local temp output file to actual output file name
          localFileSystem.rename(new Path(temp_outputPathFileName), new Path(outputPathFileName))
        }else{
          log.info("Writing output to hdfs directory : " + outputDirectoryPath)

          //For append mode
          if(csvOutputProps.saveMode.equalsIgnoreCase("append")){
            //If output directory is present and if it has any content, it means it is not first run
            if(isHdfsDirExists(outputDirectoryPath) && !isHdfsDirEmpty(outputDirectoryPath)){
              //Set header false explicitly.
              log.info("Setting header false explicitly in write.")
              val csvOptionsWithFalseHeader = getCsvOptionsWithGivenHeaderValue("false")
              //Write dataframe output to output directory
              logCsvOptions(csvOptionsWithFalseHeader)
              df.write.mode(csvOutputProps.saveMode).options(csvOptionsWithFalseHeader).csv(outputDirectoryPath)
            }
            //If directory is not present or it is empty, then it is first run
            else{
              //Write dataframe as it is
              //Delete output directory if present
              checkAndDeleteHdfsDir(outputDirectoryPath)
              //Write dataframe output to output directory
              logCsvOptions(csvOutputProps.csvOptions)
              df.write.mode(csvOutputProps.saveMode).options(csvOutputProps.csvOptions).csv(outputDirectoryPath)
            }
          }
          //For overwrite mode
          else{
            //Delete output directory if present
            checkAndDeleteHdfsDir(outputDirectoryPath)
            //Write dataframe output to output directory
            logCsvOptions(csvOutputProps.csvOptions)
            df.write.mode(csvOutputProps.saveMode).options(csvOutputProps.csvOptions).csv(outputDirectoryPath)
          }
        }
      case None => log.error(s"CSV file configuration were not provided")
    }
  }

  private def getFinalCsvOptions(): Map[String, String] = {
    //Set default csv options
    val defaultCSVOptions = Map("escape" -> "\"", "quoteAll" -> "true", "header" -> "true")

    //Read csv options from yaml file
    var metricCSVOptions = Map[String,String]()
    if (props.contains("csvOptions")){
      val csvOptionsInput = props("csvOptions").toString.trim
      for (tempCsvOutputValue <- csvOptionsInput.split(",")) {
        metricCSVOptions += (tempCsvOutputValue.split(":")(0) -> tempCsvOutputValue.split(":")(1))
      }
    }

    val csvWriterOptions = defaultCSVOptions ++ metricCSVOptions
    csvWriterOptions
  }

  private def isLocalDirectoryOutput(outputDirectoryPath: String) : Boolean = {
    if(outputDirectoryPath.contains("file:///")){
      true
    }else{
      false
    }
  }

  private def checkAndDeleteLocalFile(localFilePathName: String): Unit = {
    val localOutputFilePath = new Path(localFilePathName)
    if(localFileSystem.exists(localOutputFilePath)){
      log.info("Local file exists : " + localFilePathName + ". Deleting it first")
      localFileSystem.delete(localOutputFilePath, false)
    }else{
      log.info("Local file does not exist : " + localFilePathName)
    }
  }

  private def checkAndDeleteHdfsDir(hdfsDirPathName: String): Unit = {
    val hdfsDirPath = new Path(hdfsDirPathName)
    if(hdfsFileSystem.exists(hdfsDirPath)){
      log.info("Hdfs directory exists : " + hdfsDirPathName + ". Deleting it first")
      hdfsFileSystem.delete(hdfsDirPath, true)
    }else{
      log.info("Hdfs directory does not exist : " + hdfsDirPathName)
    }
  }

  private def isLocalFileExists(localFilePathName : String) : Boolean = {
    if(localFileSystem.exists(new Path(localFilePathName))){
      log.info("Local file exists : " + localFilePathName)
      true
    }else{
      log.info("Local file does not exist : " + localFilePathName)
      false
    }
  }

  private def isHeaderEnabled(csvOptions: Map[String, String]): Boolean = {
    var result = false
    for((csvOptName, csvOptValue) <- csvOptions){
      if(csvOptName.equalsIgnoreCase("header")){
        if((csvOptValue.equalsIgnoreCase("true"))){
          result = true
        }
      }
    }
    result
  }

  private def getPrevDataFrameWithFirstLineSkipFromLocalFile(localFilePath: String, schema: StructType): DataFrame = {
    log.info("Getting dataframe by skipping header line from file : " + localFilePath)
    val spark = Session.getSparkSession
    val csvOptionsWithHeaderFalse = getCsvOptionsWithGivenHeaderValue("false")
    logCsvOptions(csvOptionsWithHeaderFalse)
    val prevDataFrame = spark.read.format("csv").schema(schema).options(csvOptionsWithHeaderFalse).load(localFilePath)
    //Filter out row with values for all columns as null
    val finalDf = prevDataFrame.na.drop("all")
    finalDf
  }

  private def getPrevDataFrameWithoutFirstLineSkipFromLocalFile(localFilePath: String, schema: StructType): DataFrame = {
    log.info("Getting dataframe from file : " + localFilePath)
    val spark = Session.getSparkSession
    val csvOptionsWithHeaderFalse = getCsvOptionsWithGivenHeaderValue("false")
    logCsvOptions(csvOptionsWithHeaderFalse)
    val prevDataFrame = spark.read.format("csv").schema(schema).options(csvOptionsWithHeaderFalse).load(localFilePath)
    prevDataFrame
  }

  private def getCsvOptionsWithGivenHeaderValue(headerOptionValue: String): Map[String, String] = {
    log.info("Setting header to value : " + headerOptionValue)
    var csvOptionsWithGivenHeaderValue = scala.collection.mutable.Map[String, String]()
    for ((optName, optValue) <- csvOutputProps.csvOptions) {
      if (optName.equalsIgnoreCase("header")) {
        csvOptionsWithGivenHeaderValue += (optName -> headerOptionValue)
      } else {
        csvOptionsWithGivenHeaderValue += (optName -> optValue)
      }
    }
    csvOptionsWithGivenHeaderValue.toMap
  }

  private def isHdfsDirExists(hdfsDirPathName: String): Boolean = {
    val hdfsDirPath = new Path(hdfsDirPathName)
    if(hdfsFileSystem.exists(hdfsDirPath)){
      log.info("Hdfs directory exists : " + hdfsDirPathName)
      true
    }else{
      log.info("Hdfs directory does not exist : " + hdfsDirPathName)
      false
    }
  }

  private def isHdfsDirEmpty(hdfsDirPathName: String): Boolean = {
    val contents = hdfsFileSystem.listFiles(new Path(hdfsDirPathName), true)
    var contentCount = 0
    while(contents.hasNext){
      val content = contents.next()
      //Ignore success file
      if(!content.getPath.getName.equalsIgnoreCase("_SUCCESS")){
        contentCount = contentCount+1
      }
    }
    if(contentCount == 0) {
      log.info("Hdfs directory has no contents => " + hdfsDirPathName)
      true
    }
    else{
      log.info("Hdfs directory has contents => " + hdfsDirPathName + " : " + contentCount)
      false
    }
  }

  private def logCsvOptions(csvOptions: Map[String, String]): Unit = {
    log.info("Using csv options =>")
    for((optName, optValue) <- csvOptions){
      log.info(optName + ":" + optValue)
    }
  }
}
