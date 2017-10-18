/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.api.services

import slamdata.Predef._
import quasar.api._
import quasar.contrib.pathy._
import quasar.effect.{Writer, _}
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.metastore.MetaStoreFixture
import quasar.fs.mount._
import quasar.fs.mount.cache.{VCache, ViewCache}, VCache.{VCacheKVS, VCacheExpW}
import quasar.fs.ReadFile.ReadHandle
import quasar.metastore.H2MetaStoreFixture

import java.time.Instant

import doobie.imports.ConnectionIO
import scalaz.{Failure => _, Zip =>_, _}, Scalaz._
import scalaz.concurrent.Task

import eu.timepit.refined.auto._

trait VCacheFixture extends H2MetaStoreFixture {
  import InMemory._

  // TODO: Move Eff back to DataServiceSpec?
  type Eff3[A] = Coproduct[FileSystemFailure, FileSystem, A]
  type Eff2[A] = Coproduct[VCacheExpW, Eff3, A]
  type Eff1[A] = Coproduct[VCacheKVS, Eff2, A]
  type Eff0[A] = Coproduct[Timing, Eff1, A]
  type Eff[A]  = Coproduct[Task, Eff0, A]
  type EffM[A] = Free[Eff, A]

  type ViewEff[A] =
    (PathMismatchFailure :\: MountingFailure :\: Mounting :\: ViewState :\: MonotonicSeq :/: Eff)#M[A]

  val vcacheInterp: Task[VCacheKVS ~> Task] = KeyValueStore.impl.default[AFile, ViewCache]

  val vcache = VCacheKVS.Ops[ViewEff]

  def timingInterp(i: Instant) = λ[Timing ~> Task] {
    case Timing.Timestamp => Task.now(i)
    case Timing.Nanos     => Task.now(0)
  }

  def evalViewTest[A](
    now: Instant, mounts: Map[APath, MountConfig], inMemState: InMemState
  )(
    p: (ViewEff ~> Task, ViewEff ~> ResponseOr) => Task[A]
  ): Task[A] = {
    def viewFs: Task[ViewEff ~> Task] =
      (runFs(inMemState)                         ⊛
       TaskRef(mounts)                           ⊛
       TaskRef(Map.empty[ReadHandle, ResultSet]) ⊛
       MonotonicSeq.fromZero                     ⊛
       TaskRef(List.empty[VCache.Expiration])    ⊛
       MetaStoreFixture.createNewTestTransactor()
      ) { (fs, m, vs, s, r, t) =>
        val mountingInter =
          foldMapNT(KeyValueStore.impl.fromTaskRef(m)) compose Mounter.trivial[MountConfigs]

        val viewInterpF: ViewEff ~> Free[ViewEff, ?] =
          injectFT[PathMismatchFailure, ViewEff] :+:
          injectFT[MountingFailure, ViewEff]     :+:
          injectFT[Mounting, ViewEff]            :+:
          injectFT[ViewState, ViewEff]           :+:
          injectFT[MonotonicSeq, ViewEff]        :+:
          injectFT[Task, ViewEff]                :+:
          injectFT[Timing, ViewEff]              :+:
          injectFT[VCacheKVS, ViewEff]           :+:
          injectFT[VCacheExpW, ViewEff]          :+:
          injectFT[FileSystemFailure, ViewEff]   :+:
          view.fileSystem[ViewEff]

        val cw: VCacheExpW ~> Task =
          Writer.fromTaskRef(r)

        val vc: VCacheKVS ~> Task =
          foldMapNT(
            (fs compose injectNT[ManageFile, FileSystem]) :+:
            Failure.toRuntimeError[Task, FileSystemError] :+:
            t.trans                                       :+:
            cw
          ) compose
            VCache.interp[(ManageFile :\: FileSystemFailure :\: ConnectionIO :/: VCacheExpW)#M]

        val viewInterp: ViewEff ~> Task =
          Failure.toRuntimeError[Task, Mounting.PathTypeMismatch] :+:
          Failure.toRuntimeError[Task, MountingError]             :+:
          mountingInter                                           :+:
          KeyValueStore.impl.fromTaskRef(vs)                      :+:
          s                                                       :+:
          reflNT[Task]                                            :+:
          timingInterp(now)                                       :+:
          vc                                                      :+:
          cw                                                      :+:
          Failure.toRuntimeError[Task, FileSystemError]           :+:
          fs

        foldMapNT(viewInterp) compose viewInterpF
      }

    viewFs >>= (fs => p(fs, liftMT[Task, ResponseT] compose fs))
  }
}

object VCacheFixture extends VCacheFixture {
  val schema: quasar.db.Schema[Int] = quasar.metastore.Schema.schema
}
