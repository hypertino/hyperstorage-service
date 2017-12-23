/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package akka.actor

trait FSMEx[S, D] extends FSM[S, D] {
  this: Actor ⇒
  private[akka] override def processEvent(event: Event, source: AnyRef): Unit = {
    processEventEx(event, source)
  }

  protected def processEventEx(event: Event, source: AnyRef): Unit = {
    super.processEvent(event, source)
  }
}
