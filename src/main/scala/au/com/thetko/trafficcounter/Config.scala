package au.com.thetko.trafficcounter

import org.rogach.scallop._

class Config(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = trailArg[String](descr = "Input CSV file", required = true)
  verify()
}