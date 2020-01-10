/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl

import slamdata.Predef._

import cats.effect.{Resource, Sync}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.instances.option._

trait ResourceManager[F[_], I, A] {
  // Having a pair of A and its finalizer F[Unit] cache it at the `I` index.
  // This uses pair instead of `Resource` to omit `Bracket` constraint (might be useful for mocking)
  def manage(i: I, allocated: (A, F[Unit])): F[Unit]
  // Remove cached value from `I` and call the finalizer
  def shutdown(i: I): F[Unit]
  // Get the value by index
  def get(i: I): F[Option[A]]
}

object ResourceManager {
  // Creates a resource which finalizer is actually calls all finalizers of managed pairs
  def apply[F[_]: Sync, I, A]: Resource[F, ResourceManager[F, I, A]] = {
    val fPair = Ref.of[F, Map[I, (A, F[Unit])]](Map.empty) map { ref =>
      val mgr = new ResourceManager[F, I, A] {
        // Replaces allocated resource at index. To do this we have to shutdown previously allocated resource
        def manage(i: I, allocated: (A, F[Unit])): F[Unit] =
          shutdown(i) >> ref.update(_.updated(i, allocated))

        def shutdown(i: I): F[Unit] = for {
          current <- ref.get
          _ <- current.get(i).traverse_(_._2)
          _ <- ref.update(_ - i)
        } yield ()

        def get(i: I): F[Option[A]] =
          ref.get map { x => x.get(i).map(_._1) }
      }
      (ref, mgr)
    }
    val rPair = Resource.make(fPair) { case (ref, _) => ref.get flatMap { (mp: Map[I, (A, F[Unit])]) =>
      val finalizers = mp.toList.map(x => x._2._2)
      finalizers.foldLeft(().pure[F]){ (ef: F[Unit], inc: F[Unit]) =>
        ef.guarantee(inc)
      }
    }}
    rPair.map(_._2)
  }
}
