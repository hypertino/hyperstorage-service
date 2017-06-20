package com.hypertino.hyperstorage

import com.hypertino.hyperstorage.modules.ModuleAggregator
import com.hypertino.service.control.api.ServiceController
import scaldi.Injectable

object MainApp extends Injectable {
  def main(args: Array[String]): Unit = {
    implicit val injector = ModuleAggregator.injector
    inject[ServiceController].run()
  }
}
