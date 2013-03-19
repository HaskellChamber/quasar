/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.shard

import com.precog.util.PrecogUnit
import com.precog.muspelheim._

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.jobs.JobManager
import com.precog.common.security._
import com.precog.daze._
import com.precog.shard.service._

import akka.dispatch.{Future, ExecutionContext, Promise}
import akka.util.{Duration, Timeout}

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.data.ByteChunk._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.BlueEyesServiceBuilder

import blueeyes.core.data.{ DefaultBijections, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService

import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import DefaultBijections._

import java.nio.{ CharBuffer, ByteBuffer }
import java.nio.charset.{ Charset, CoderResult }

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._
import scalaz.syntax.monad._
import scalaz.std.function._

sealed trait ShardStateOptions
object ShardStateOptions {
  case object NoOptions extends ShardStateOptions
  case object DisableAsyncQueries extends ShardStateOptions
}

sealed trait ShardState {
  def platform: Platform[Future, StreamT[Future, CharBuffer]]
  def apiKeyFinder: APIKeyFinder[Future]
  def stoppable: Stoppable
}

case class ManagedQueryShardState(
  override val platform: ManagedPlatform,
  apiKeyFinder: APIKeyFinder[Future],
  jobManager: JobManager[Future],
  clock: Clock,
  options: ShardStateOptions = ShardStateOptions.NoOptions,
  stoppable: Stoppable) extends ShardState

case class BasicShardState(
  platform: Platform[Future, StreamT[Future, CharBuffer]],
  apiKeyFinder: APIKeyFinder[Future],
  stoppable: Stoppable) extends ShardState

trait ShardService extends
    BlueEyesServiceBuilder with
    ShardServiceCombinators with
    Logging {

  import ShardStateOptions.DisableAsyncQueries

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  val timeout = Timeout(120000)
  /**
   * This provides the configuration for the service and expects a `ShardState`
   * in return. The exact `ShardState` can be either a `BasicShardState` or a
   * `ManagedQueryShardState`. If a `ManagedQueryShardState` is returned, then
   * all features, including async queries and detailed query output are
   * enabled. Otherwise, if only a `BasicShardState` is returned, then only
   * basic synchronous queries will be allowed.
   *
   * On service startup, the platform's `startup` method will be
   * called.
   */
  def configureShardState(config: Configuration): Future[ShardState]

  val utf8 = Charset.forName("UTF-8")
  val BufferSize = 64 * 1024

  def optionsResponse = M.point(
    HttpResponse[ByteChunk](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  private def bufferOutput(stream0: StreamT[Future, CharBuffer]) = {
    val encoder = utf8.newEncoder()

    def loop(stream: StreamT[Future, CharBuffer], buf: ByteBuffer): StreamT[Future, ByteBuffer] = {
      StreamT(stream.uncons map {
        case Some((cbuf, tail)) =>
          val result = encoder.encode(cbuf, buf, false)
          if (result == CoderResult.OVERFLOW) {
            buf.flip()
            StreamT.Yield(buf, loop(cbuf :: tail, ByteBuffer.allocate(BufferSize)))
          } else {
            StreamT.Skip(loop(tail, buf))
          }

        case None =>
          val result = encoder.encode(CharBuffer.wrap(""), buf, true)
          buf.flip()

          if (result == CoderResult.OVERFLOW) {
            StreamT.Yield(buf, loop(stream, ByteBuffer.allocate(BufferSize)))
          } else {
            StreamT.Yield(buf, StreamT.empty)
          }
      })
    }

    loop(stream0, ByteBuffer.allocate(BufferSize))
  }

  private def queryResultToByteChunk: QueryResult => ByteChunk = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Left(ByteBuffer.wrap(jv.renderCompact.getBytes(utf8)))
      case Right(stream) => Right(bufferOutput(stream))
    }
  }

  private def asyncQueryService(state: ShardState) = state match {
    case BasicShardState(_, _, _) | ManagedQueryShardState(_, _, _, _, DisableAsyncQueries, _) =>
      new QueryServiceNotAvailable
    case ManagedQueryShardState(platform, _, _, _, _, _) =>
      new AsyncQueryServiceHandler(platform.asynchronous)
  }

  private def syncQueryService(state: ShardState) = state match {
    case BasicShardState(platform, _, _) =>
      new BasicQueryServiceHandler(platform)
    case ManagedQueryShardState(platform, _, jobManager, _, _, _) =>
      new SyncQueryServiceHandler(platform.synchronous, jobManager, SyncResultFormat.Simple)
  }

  private def cf = implicitly[ByteChunk => Future[JValue]]

  def shardService[F[+_]](service: HttpService[Future[JValue], F[Future[HttpResponse[QueryResult]]]])(implicit
      F: Functor[F]): HttpService[ByteChunk, F[Future[HttpResponse[ByteChunk]]]] = {
    service map { _ map { _ map { _ map queryResultToByteChunk } } } contramap cf
  }

  private def asyncHandler(state: ShardState) = {
    val queryHandler =
      path("/analytics") {
        jsonAPIKey(state.apiKeyFinder) {
          path("/queries") {
            shardService[({ type λ[+α] = (APIKey => α) })#λ] {
              asyncQuery(post(asyncQueryService(state)))
            }
          }
        }
      }

    state match {
      case ManagedQueryShardState(_, apiKeyFinder, jobManager, clock, _, _) =>
        path("/analytics") {
          jsonAPIKey(apiKeyFinder) {
            path("/queries") {
              path("/'jobId") {
                get(new AsyncQueryResultServiceHandler(jobManager)) ~
                delete(new QueryDeleteHandler[ByteChunk](jobManager, clock))
              }
            }
          }
        } ~ queryHandler

      case _ =>
        queryHandler
    }
  }

  private def syncHandler(state: ShardState) = {
    jsonp[ByteChunk] {
      jsonAPIKey(state.apiKeyFinder) {
        dataPath("/analytics/fs") {
          query[ByteChunk, HttpResponse[ByteChunk]] {
            get {
              shardService[({ type λ[+α] = ((APIKey, Path, String, QueryOptions) => α) })#λ] {
                syncQueryService(state)
              }
            } ~
            options {
              (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path, s: String, o: QueryOptions) => optionsResponse
            }
          }
        } ~
        dataPath("/meta/fs") {
          get {
            shardService[({ type λ[+α] = ((APIKey, Path) => α) })#λ] {
              new BrowseServiceHandler[Future[JValue]](state.platform.metadataClient, state.apiKeyFinder)
            }
          } ~
          options {
            (request: HttpRequest[ByteChunk]) => (a: APIKey, p: Path) => optionsResponse
          }
        }
      }
    }
  }


  lazy val analyticsService = this.service("analytics", "2.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          logger.info("Starting shard with config:\n" + context.config)
          configureShardState(context.config)
        } ->
        request { state =>
          implicit val compression = CompressService.defaultCompressions
          compress { 
            asyncHandler(state) ~ syncHandler(state) 
          }
        } ->
        stop { state: ShardState =>
          state.stoppable
        }
      }
    }
  }
}
