/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.impl.external

import quasar.connector.destination.DestinationModule
import quasar.connector.datasource.LightweightDatasourceModule
import quasar.connector.scheduler.SchedulerModule

import slamdata.Predef._

// this trait exists mostly because we can't have a sealed algebra that covers *all* modules, so we project one here
sealed trait ExternalModule extends Product with Serializable

object ExternalModule {
  final case class Datasource(mod: LightweightDatasourceModule) extends ExternalModule
  final case class Destination(mod: DestinationModule) extends ExternalModule
  final case class Scheduler(mod: SchedulerModule) extends ExternalModule

  def unapply(ar: AnyRef): Option[ExternalModule] =
    wrap.lift(ar)

  val wrap: PartialFunction[AnyRef, ExternalModule] = {
    case lw: LightweightDatasourceModule =>
      Datasource(lw)

    case dm: DestinationModule =>
      Destination(dm)

    case sm: SchedulerModule =>
      Scheduler(sm)
  }
}
