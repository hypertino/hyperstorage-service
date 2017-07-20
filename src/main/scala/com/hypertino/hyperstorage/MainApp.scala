package com.hypertino.hyperstorage

import com.hypertino.hyperstorage.modules.{HyperStorageServiceModule, SystemServiceModule}
import com.hypertino.service.config.ConfigModule
import com.hypertino.service.control.api.ServiceController
import scaldi.Injectable

object MainApp extends Injectable {
  def main(args: Array[String]): Unit = {
    implicit val injector = new SystemServiceModule :: new HyperStorageServiceModule :: ConfigModule()
    inject[ServiceController].run()
  }
}
