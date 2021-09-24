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

import slamdata.Predef.{Eq => _, _}

import java.time.{LocalDateTime, LocalDate, OffsetDateTime, ZoneOffset}

import cats.{Eq, Id, Show}
import cats.data.Const
import cats.evidence._
import cats.implicits._

import qdata.time.OffsetDate

import spire.math.Real

sealed trait OffsetKey[F[_], A] extends Product with Serializable {
  type Repr
  val value: F[A]
  val reify: A === Repr
}
sealed trait InternalKey[F[_], A] extends OffsetKey[F, A]

object InternalKey {
  type Actual[A] = InternalKey[Id, A]
  type Formal[T, A] = InternalKey[Const[T, ?], A]

  type RealKey[F[_]] = OffsetKey.RealKey[F]
  val RealKey = OffsetKey.RealKey
  type StringKey[F[_]] = OffsetKey.StringKey[F]
  val StringKey = OffsetKey.StringKey
  type DateTimeKey[F[_]] = OffsetKey.DateTimeKey[F]
  val DateTimeKey = OffsetKey.DateTimeKey
  type LocalDateKey[F[_]] = OffsetKey.LocalDateKey[F]
  val LocalDateKey = OffsetKey.LocalDateKey
  type LocalDateTimeKey[F[_]] = OffsetKey.LocalDateTimeKey[F]
  val LocalDateTimeKey = OffsetKey.LocalDateTimeKey
  type DateKey[F[_]] = OffsetKey.DateKey[F]
  val DateKey = OffsetKey.DateKey

  object Actual {
    def real(k: Real): Actual[Real] =
      RealKey[Id](k)

    def string(k: String): Actual[String] =
      StringKey[Id](k)

    def dateTime(k: OffsetDateTime): Actual[OffsetDateTime] =
      DateTimeKey[Id](k)

    def date(k: OffsetDate): Actual[OffsetDate] =
      DateKey[Id](k)

    def localDate(k: LocalDate): Actual[LocalDate] =
      LocalDateKey[Id](k)

    def localDateTime(k: LocalDateTime): Actual[LocalDateTime] =
      LocalDateTimeKey[Id](k)
  }

  object Formal {
    def real[T](t: T): Formal[T, Real] =
      RealKey(Const[T, Real](t))

    def string[T](t: T): Formal[T, String] =
      StringKey(Const[T, String](t))

    def dateTime[T](t: T): Formal[T, OffsetDateTime] =
      DateTimeKey(Const[T, OffsetDateTime](t))

    def date[T](t: T): Formal[T, OffsetDate] =
      DateKey(Const[T, OffsetDate](t))

    def localDate[T](t: T): Formal[T, LocalDate] =
      LocalDateKey(Const[T, LocalDate](t))

    def localDateTime[T](t: T): Formal[T, LocalDateTime] =
      LocalDateTimeKey(Const[T, LocalDateTime](t))
  }

  def fromOffset[F[_], A](inp: OffsetKey[F, A]): Option[InternalKey[F, A]] = inp match {
    case i: InternalKey[F, A] => Some(i)
    case _ => None
  }
}
object OffsetKey {
  type Actual[A] = OffsetKey[Id, A]
  type Formal[T, A] = OffsetKey[Const[T, ?], A]

  final case class RealKey[F[_]](value: F[Real]) extends InternalKey[F, Real] {
    type Repr = Real
    val reify = Is.refl
  }

  final case class StringKey[F[_]](value: F[String]) extends InternalKey[F, String] {
    type Repr = String
    val reify = Is.refl
  }

  final case class DateTimeKey[F[_]](value: F[OffsetDateTime]) extends InternalKey[F, OffsetDateTime] {
    type Repr = OffsetDateTime
    val reify = Is.refl
  }

  final case class LocalDateKey[F[_]](value: F[LocalDate]) extends InternalKey[F, LocalDate] {
    type Repr = LocalDate
    val reify = Is.refl
  }

  final case class LocalDateTimeKey[F[_]](value: F[LocalDateTime]) extends InternalKey[F, LocalDateTime] {
    type Repr = LocalDateTime
    val reify = Is.refl
  }

  final case class DateKey[F[_]](value: F[OffsetDate]) extends InternalKey[F, OffsetDate] {
    type Repr = OffsetDate
    val reify = Is.refl
  }

  final case class ExternalKey[F[_]](value: F[ExternalOffsetKey]) extends OffsetKey[F, ExternalOffsetKey] {
    type Repr = ExternalOffsetKey
    val reify = Is.refl
  }

  object Actual {
    def real(k: Real): Actual[Real] =
      RealKey[Id](k)

    def string(k: String): Actual[String] =
      StringKey[Id](k)

    def dateTime(k: OffsetDateTime): Actual[OffsetDateTime] =
      DateTimeKey[Id](k)

    def external(k: ExternalOffsetKey): Actual[ExternalOffsetKey] =
      ExternalKey[Id](k)

    def localDateTime(k: LocalDateTime): Actual[LocalDateTime] =
      LocalDateTimeKey[Id](k)

    def date(k: OffsetDate): Actual[OffsetDate] =
      DateKey[Id](k)

    def localDate(k: LocalDate): Actual[LocalDate] =
      LocalDateKey[Id](k)
  }

  object Formal {
    def real[T](t: T): Formal[T, Real] =
      RealKey(Const[T, Real](t))

    def string[T](t: T): Formal[T, String] =
      StringKey(Const[T, String](t))

    def dateTime[T](t: T): Formal[T, OffsetDateTime] =
      DateTimeKey(Const[T, OffsetDateTime](t))

    def external[T](t: T): Formal[T, ExternalOffsetKey] =
      ExternalKey(Const[T, ExternalOffsetKey](t))

    def date[T](t: T): Formal[T, OffsetDate] =
      DateKey(Const[T, OffsetDate](t))

    def localDate[T](t: T): Formal[T, LocalDate] =
      LocalDateKey(Const[T, LocalDate](t))

    def localDateTime[T](t: T): Formal[T, LocalDateTime] =
      LocalDateTimeKey(Const[T, LocalDateTime](t))
  }

  implicit def offsetKeyActualShow[A]: Show[Actual[A]] =
    Show show {
      case RealKey(k) => s"RealKey($k)"
      case StringKey(k) => s"StringKey($k)"
      case DateTimeKey(k) => s"DateTimeKey($k)"
      case DateKey(k) => s"DateKey($k)"
      case LocalDateKey(k) => s"LocalDateKey($k)"
      case LocalDateTimeKey(k) => s"LocalDateTimeKey($k)"
      case ExternalKey(k) => s"ExternalKey(${k.show})"
    }

  implicit def offsetKeyFormalShow[T: Show, A]: Show[Formal[T, A]] =
    Show show {
      case k: RealKey[Const[T, ?]] => s"RealKey(${k.value.getConst.show})"
      case k: StringKey[Const[T, ?]] => s"StringKey(${k.value.getConst.show})"
      case k: DateTimeKey[Const[T, ?]] => s"DateTimeKey(${k.value.getConst.show})"
      case k: LocalDateKey[Const[T, ?]] => s"LocalDateKey(${k.value.getConst.show})"
      case k: DateKey[Const[T, ?]] => s"DateKey(${k.value.getConst.show})"
      case k: LocalDateTimeKey[Const[T, ?]] => s"LocalDateTimeKey(${k.value.getConst.show})"
      case k: ExternalKey[Const[T, ?]] => s"ExternalKey(${k.value.getConst.show})"
    }

  implicit def offsetKeyActualEq[A]: Eq[Actual[A]] = {
    implicit val realEq: Eq[Real] = Eq.fromUniversalEquals
    implicit val offsetDateTimeEq: Eq[OffsetDateTime] = Eq.fromUniversalEquals
    implicit val localDateEq: Eq[LocalDate] = Eq.fromUniversalEquals
    implicit val localDateTimeEq: Eq[LocalDateTime] = Eq.fromUniversalEquals
    implicit val zoneOffsetEq: Eq[ZoneOffset] = Eq.fromUniversalEquals
    implicit val offsetDateEq: Eq[OffsetDate] = Eq.by(x => (x.date, x.offset))

    new Eq[Actual[A]] {
      def eqv(a: Actual[A], b: Actual[A]) = a match {
        case ak: RealKey[Id] => b match {
          case bk: RealKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: StringKey[Id] => b match {
          case bk: StringKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: DateKey[Id] => b match {
          case bk: DateKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: DateTimeKey[Id] => b match {
          case bk: DateTimeKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: LocalDateKey[Id] => b match {
          case bk: LocalDateKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: LocalDateTimeKey[Id] => b match {
          case bk: LocalDateTimeKey[Id] => ak.value === bk.value
          case _ => false
        }
        case ak: ExternalKey[Id] => b match {
          case bk: ExternalKey[Id] => ak.value === bk.value
          case _ => false
        }
      }
    }
  }

  implicit def offsetKeyFormalEq[T: Eq, A]: Eq[Formal[T, A]] =
    Eq.by[Formal[T, A], (Int, T)] {
      case k: RealKey[Const[T, ?]] => (0, k.value.getConst)
      case k: StringKey[Const[T, ?]] => (1, k.value.getConst)
      case k: DateTimeKey[Const[T, ?]] => (2, k.value.getConst)
      case k: ExternalKey[Const[T, ?]] => (3, k.value.getConst)
      case k: DateKey[Const[T, ?]] => (4, k.value.getConst)
      case k: LocalDateKey[Const[T, ?]] => (5, k.value.getConst)
      case k: LocalDateTimeKey[Const[T, ?]] => (6, k.value.getConst)
    }
}
