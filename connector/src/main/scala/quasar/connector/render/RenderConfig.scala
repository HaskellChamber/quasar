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

package quasar.connector.render

import scala._

import java.lang.{CharSequence, String}
import java.time.format.DateTimeFormatter

sealed trait RenderConfig[A] extends Product with Serializable

object RenderConfig {
  import DateTimeFormatter._

  // please note that binary compatibility is *only* guaranteed on this if you
  // construct instances based on named arguments
  final case class Csv(
      includeHeader: Boolean = true,
      nullSentinel: Option[String] = None,
      includeBom: Boolean = true,
      booleanFormat: Boolean => String = if (_) "true" else "false",
      numericScale: Option[Int] = None, // None means render the value as-is
      offsetDateTimeFormat: DateTimeFormatter = ISO_DATE_TIME,
      offsetDateFormat: DateTimeFormatter = ISO_OFFSET_DATE,
      offsetTimeFormat: DateTimeFormatter = ISO_OFFSET_TIME,
      localDateTimeFormat: DateTimeFormatter = ISO_LOCAL_DATE_TIME,
      localDateFormat: DateTimeFormatter = ISO_LOCAL_DATE,
      localTimeFormat: DateTimeFormatter = ISO_LOCAL_TIME)
      extends RenderConfig[Byte]

  final case class Json(
      prefix: String,
      delimiter: String,
      suffix: String)
      extends RenderConfig[Byte]

  final case class Separated(
      separator: CharSequence,
      columnRender: ColumnRender[CharSequence])
      extends RenderConfig[CharSequence]
}
