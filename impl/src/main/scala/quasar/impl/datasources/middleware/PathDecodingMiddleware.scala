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

package quasar.impl.datasources.middleware

import quasar.api.resource._
import quasar.connector.datasource.Datasource
import quasar.impl.QuasarDatasource
import quasar.qscript.InterpretedRead

import cats.effect.{Resource, Sync}

import scala._, Predef._
import scala.util.matching.Regex
import java.lang.String

object PathDecodingMiddleware {
  def apply[F[_]: Sync, G[_], R, P <: ResourcePathType, I](
      datasourceId: I,
      inp: QuasarDatasource[Resource[F, *], G, R, P])
      : F[QuasarDatasource[Resource[F, *], G, R, P]] = Sync[F].delay {
    new Datasource[Resource[F, *], G, InterpretedRead[ResourcePath], R, P] {
      val kind = inp.kind
      val loaders = inp.loaders.map(_.contramap { (ir: InterpretedRead[ResourcePath]) =>
        ir.copy(path = decodeResourcePath(ir.path))
      })
      def pathIsResource(p: ResourcePath) =
        inp.pathIsResource(decodeResourcePath(p))
      def prefixedChildPaths(pfx: ResourcePath) =
        inp.prefixedChildPaths(decodeResourcePath(pfx))
    }
  }

  private def decodeResourcePath: ResourcePath => ResourcePath =
    ResourcePath.resourceNamesIso.modify(_.map {
      case ResourceName(n) => ResourceName(decodeStr(n))
    })

  private def decodeStr(inp: String): String = {
    inp
      .replaceAll(Regex.quote("$$slash"), SlashMark)
      .replaceAll(Regex.quote("$$dot"), DotMark)
      .replaceAll(Regex.quote("$slash"), Regex.quoteReplacement("/"))
      .replaceAll(Regex.quote("$dot"), Regex.quoteReplacement("."))
      .replaceAll(SlashMark, Regex.quoteReplacement("$slash"))
      .replaceAll(DotMark, Regex.quoteReplacement("$dot"))
  }

  val SlashMark: String = "bf9028bb-0420-415e-aca6-829404017942"
  val DotMark: String = "f656c951-9d1e-4c2c-8462-b93be61ffe31"
}
