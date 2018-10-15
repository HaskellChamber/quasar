/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir.evaluate

import quasar.ParseInstructionSpec

import tectonic.{AsyncParser, Plate}
import tectonic.test.{Event, ReifiedTerminalPlate}

object TectonicParseInstructionSpec
    extends ParseInstructionSpec.PivotSpec
    with ParseInstructionSpec.IdsSpec {

  type JsonStream = List[Event]

  def evalIds(stream: JsonStream): JsonStream =
    evalPlate(stream)(new IdsPlate(_))

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream =
    evalPlate(stream)(new PivotPlate(pivot, _))

  private def evalPlate(stream: JsonStream)(f: Plate[List[Event]] => Plate[List[Event]]): JsonStream = {
    val plate = f(new ReifiedTerminalPlate)
    stripNoOps(ReifiedTerminalPlate.visit(stream, plate))
  }

  // remove no-op nesting (paired adjacent nest/unnest)
  private def stripNoOps(stream: JsonStream): JsonStream = {
    import Event._

    val (_, backV) = stream.foldLeft((Vector[Event](), Vector[Event]())) {
      case ((Vector(), acc), Unnest) => (Vector(), acc :+ Unnest)

      case ((buffer, acc), n @ NestMap(_)) => (buffer :+ n, acc)
      case ((buffer, acc), NestArr) => (buffer :+ NestArr, acc)
      case ((buffer, acc), n @ NestMeta(_)) => (buffer :+ n, acc)

      case ((buffer, acc), Unnest) => (buffer.dropRight(1), acc)

      case ((buffer, acc), e) => (Vector(), acc ++ buffer :+ e)
    }

    backV.toList
  }

  protected def ldjson(str: String): JsonStream = {
    val plate = new ReifiedTerminalPlate
    val parser = AsyncParser(plate, AsyncParser.ValueStream)

    val events1 = parser.absorb(str).right.get
    val events2 = parser.finish().right.get

    events1 ::: events2
  }
}
