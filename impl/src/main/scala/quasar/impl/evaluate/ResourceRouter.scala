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

package quasar.impl.evaluate

import slamdata.Predef._

import quasar.api.resource._
import quasar.connector.datasource.Datasource
import quasar.connector.evaluate.Source
import quasar.contrib.pathy.AFile

import cats.Applicative
import cats.data.OptionT
import cats.syntax.applicative._

import monocle.Prism

/** Translates resource paths from queries into actual sources. */
object ResourceRouter {
  val DatasourceResourcePrefix = "datasource"

  def apply[F[_]: Applicative, G[_], H[_], A, B, P <: ResourcePathType, I](
      IdString: Prism[String, I],
      lookupRunning: I => F[Option[Datasource[G, H, A, B, P]]])(
      file: AFile)
      : F[Option[Source[Datasource[G, H, A, B, P]]]] =
    ResourcePath.leaf(file) match {
      case DatasourceResourcePrefix /: IdString(id) /: srcPath =>
        OptionT(lookupRunning(id))
          .map(Source(srcPath, _))
          .value

      case _ =>
        (None: Option[Source[Datasource[G, H, A, B, P]]]).pure[F]
    }
}
