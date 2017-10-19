package com.hypertino.hyperstorage.modules

import com.hypertino.hyperbus.transport.api.ServiceRegistrator
import com.hypertino.hyperbus.transport.registrators.DummyRegistrator
import monix.execution.Scheduler
import scaldi.Module

class SystemServicesModule extends Module {
  bind[Scheduler] to monix.execution.Scheduler.Implicits.global
  bind[ServiceRegistrator] to DummyRegistrator
}
