package com.assignment.spark.Utils

import java.io.FileReader
import java.security.InvalidParameterException

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.commons.lang3.StringUtils

object ConfigReader {

  def readConfig(fileName : String ) : Config ={

    if (StringUtils.isNoneBlank(fileName)) {
      ConfigFactory.parseReader(new FileReader(fileName.trim))
    } else {
      throw new InvalidParameterException("File Name cannot be blank or null")
    }

  }

}
