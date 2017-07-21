package com.hypertino.hyperstorage.modules

import monix.execution.Scheduler
import scaldi.Module

class SystemServicesModule extends Module {
  bind[Scheduler] to monix.execution.Scheduler.Implicits.global
}
