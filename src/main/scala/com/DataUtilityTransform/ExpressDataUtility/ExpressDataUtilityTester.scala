package com.DataUtilityTransform.ExpressDataUtility

import java.nio.file.{Files, Paths}

import com.DataUtilityTransform.ExpressDataUtility.session.Session
import com.DataUtilityTransform.ExpressDataUtility.utils.TestUtils
import com.DataUtilityTransform.ExpressDataUtility.utils.TestUtils
import com.DataUtilityTransform.ExpressDataUtility.TesterConfigurationParser.ExpressDataUtilityTesterArgs
import com.DataUtilityTransform.ExpressDataUtility.utils.TestUtils
import org.apache.log4j.LogManager
import scopt.OptionParser


object ExpressDataUtilityTester extends App {
  lazy val log = LogManager.getLogger(this.getClass)
  val ExpressDataUtilityArgs = TesterConfigurationParser.parser.parse(args, ExpressDataUtilityTesterArgs()).getOrElse(ExpressDataUtilityTesterArgs())

  ExpressDataUtilityTesterArgs.settings.foreach(settings => {
    val metricTestSettings = TestUtils.getTestSettings(settings)
    val config = TestUtils.createExpressDataUtilityConfigFromTestSettings(settings, metricTestSettings, ExpressDataUtilityTesterArgs.preview)
    Session.init(config)
    TestUtils.runTests(metricTestSettings.tests)
  })

}

object TesterConfigurationParser {
  val NumberOfPreviewLines = 10

  case class ExpressDataUtilityTesterArgs(settings: Seq[String] = Seq(), preview: Int = NumberOfPreviewLines)

  val parser: OptionParser[ExpressDataUtilityTesterArgs] = new scopt.OptionParser[ExpressDataUtilityTesterArgs]("ExpressDataUtilityTester") {
    head("ExpressDataUtilityTesterRunner", "1.0")
    opt[Seq[String]]('t', "test-settings")
      .valueName("<test-setting1>,<test-setting2>...")
      .action((x, c) => c.copy(settings = x))
      .text("test settings for each metric set")
      .validate(x => {
        if (x.exists(f => !Files.exists(Paths.get(f)))) {
          failure("One of the file is not found")
        }
        else {
          success
        }
      })
      .required()
    opt[Int]('p', "preview").action((x, c) =>
      c.copy(preview = x)).text("number of preview lines for each step")
    help("help") text "use command line arguments to specify the settings for each metric set"
  }
}
