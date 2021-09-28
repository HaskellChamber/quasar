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

package quasar.api.push

import slamdata.Predef._

import qdata.time.OffsetDate

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

import cats.Id
import cats.implicits._

import skolems.∃

import spire.math.Real

import java.time._

object OffsetKeyGenerators {
  implicit def internalFormalArbitrary[T: Arbitrary]: Arbitrary[∃[InternalKey.Formal[T, *]]] = Arbitrary {
    for {
      mark <- arbitrary[T]
      result <- Gen.oneOf(List(
        ∃(InternalKey.Formal.real(mark)),
        ∃(InternalKey.Formal.string(mark)),
        ∃(InternalKey.Formal.dateTime(mark)),
        ∃(InternalKey.Formal.date(mark)),
        ∃(InternalKey.Formal.localDate(mark)),
        ∃(InternalKey.Formal.localDateTime(mark))))
    } yield result
  }

  implicit def formalArbitrary[T: Arbitrary]: Arbitrary[∃[OffsetKey.Formal[T, *]]] = Arbitrary {
    for {
      select <- Gen.choose(0, 5)
      res <- if (select =!= 0) {
        arbitrary[∃[InternalKey.Formal[T, *]]].map(x => x: ∃[OffsetKey.Formal[T, *]])
      } else {
        arbitrary[T].map(mark => ∃(OffsetKey.Formal.external(mark)))
      }
    } yield res
  }

  def localDate: Gen[LocalDate] = for {
    year <- Gen.choose(-9999, 9999)
    month <- Gen.choose(1, 12)
    day <- Gen.choose(1, 28)
  } yield LocalDate.of(year, month, day)

  def localTime: Gen[LocalTime] = for {
    hour <- Gen.choose(0, 23)
    minute <- Gen.choose(0, 59)
    seconds <- Gen.choose(0, 59)
    nanos <- Gen.choose(0, 999999999)
  } yield LocalTime.of(hour, minute, seconds, nanos)

  def localDateTime: Gen[LocalDateTime] = for {
    date <- localDate
    time <- localTime
  } yield LocalDateTime.of(date, time)

  def zoneOffset: Gen[ZoneOffset] =
    Gen.choose(-12, 14).map(ZoneOffset.ofHours(_))

  def offsetDate: Gen[OffsetDate] = for {
    date <- localDate
    offset <- zoneOffset
  } yield OffsetDate(date, offset)

  def offsetDateTime: Gen[OffsetDateTime] = for {
    date <- localDate
    time <- localTime
    offset <- zoneOffset
  } yield OffsetDateTime.of(date, time, offset)

  def externalOffsetKey: Gen[ExternalOffsetKey] =
    Gen.alphaNumStr.map(x => ExternalOffsetKey(x.getBytes))

  def realKeyGen: Gen[OffsetKey.RealKey[Id]] =
    arbitrary[Double].map(Real(_)).map(OffsetKey.RealKey[Id](_))

  def stringKeyGen: Gen[OffsetKey.StringKey[Id]] =
    Gen.alphaNumStr.map(OffsetKey.StringKey[Id](_))

  def localDateKeyGen: Gen[OffsetKey.LocalDateKey[Id]] =
    localDate.map(OffsetKey.LocalDateKey[Id](_))

  def offsetDateKeyGen: Gen[OffsetKey.DateKey[Id]] =
    offsetDate.map(OffsetKey.DateKey[Id](_))

  def localDateTimeKeyGen: Gen[OffsetKey.LocalDateTimeKey[Id]] =
    localDateTime.map(OffsetKey.LocalDateTimeKey[Id](_))

  def offsetDateTimeKeyGen: Gen[OffsetKey.DateTimeKey[Id]] =
    offsetDateTime.map(OffsetKey.DateTimeKey[Id](_))

  def externalKeyGen: Gen[OffsetKey.ExternalKey[Id]] =
    externalOffsetKey.map(OffsetKey.ExternalKey[Id](_))

  implicit def internalActualArbitrary: Arbitrary[∃[InternalKey.Actual]] = Arbitrary {
    Gen.oneOf(
      realKeyGen.map(∃(_)),
      stringKeyGen.map(∃(_)),
      localDateKeyGen.map(∃(_)),
      localDateTimeKeyGen.map(∃(_)),
      offsetDateTimeKeyGen.map(∃(_)),
      offsetDateKeyGen.map(∃(_)))
  }

  implicit def offsetActualArbitrary: Arbitrary[∃[OffsetKey.Actual]] = Arbitrary {
    Gen.oneOf(
      realKeyGen.map(∃(_)),
      stringKeyGen.map(∃(_)),
      localDateKeyGen.map(∃(_)),
      localDateTimeKeyGen.map(∃(_)),
      offsetDateTimeKeyGen.map(∃(_)),
      offsetDateKeyGen.map(∃(_)),
      externalKeyGen.map(∃(_)))
  }
}
