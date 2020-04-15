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

import quasar.api.scheduler.SchedulerModule
import quasar.impl.DatasourceModule
import quasar.connector.destination.DestinationModule
import quasar.connector.datasource.{HeavyweightDatasourceModule, LightweightDatasourceModule}

import java.util.UUID

import slamdata.Predef._

import scala.reflect.classTag

// this trait exists mostly because we can't have a sealed algebra that covers *all* modules, so we project one here
sealed trait ExternalModule[F[_]] extends Product with Serializable

object ExternalModule {
  final case class Datasource[F[_]](mod: DatasourceModule) extends ExternalModule[F]
  final case class Destination[F[_]](mod: DestinationModule) extends ExternalModule[F]
  final case class Scheduler[F[_]](mod: SchedulerModule[F, UUID]) extends ExternalModule[F]

  trait Unapply[F[_]] {
    def unapply(ar: AnyRef): Option[ExternalModule[F]]
  }

  def unapplier[F[_]]: Unapply[F] = new Unapply[F] {
    type SchedulerModuleF = SchedulerModule[F, UUID]

    def unapply(ar: AnyRef): Option[ExternalModule[F]] =
      wrap.lift(ar)

    val wrap: PartialFunction[AnyRef, ExternalModule[F]] = {
      case lw: LightweightDatasourceModule =>
        Datasource(DatasourceModule.Lightweight(lw))

      case hw: HeavyweightDatasourceModule =>
        Datasource(DatasourceModule.Heavyweight(hw))

      case dm: DestinationModule =>
        Destination(dm)

      case sm if classTag[SchedulerModule[F, UUID]].runtimeClass.isInstance(sm) =>
        Scheduler(sm.asInstanceOf[SchedulerModule[F, UUID]])
    }
  }


}
