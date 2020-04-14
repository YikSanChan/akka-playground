package akkastreams

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.{Source => IoSource}
import scala.util.Using

object WordCount extends App {
  val filepath = "src/main/resources/tinyshakespeare.txt"

  implicit val system: ActorSystem = ActorSystem("WordCount")
  implicit val ec: ExecutionContext = system.dispatcher

  val m1 = wc1()
  val m2 = wc2()
  val m3 = wc3()
  val m4 = wc4()

  // m1 is slightly different from the rest probably because they read the file slightly differently
  assert(m2 == m3)
  assert(m3 == m4)
  println("done")

  // in memory
  def wc1(): Map[String, Int] = {
    Using(IoSource.fromFile(filepath)) { src =>
      src
        .getLines()
        .flatMap(_.split("\\W+"))
        .map(_.toLowerCase())
        .toList
        .groupMapReduce(identity)(_ => 1)(_ + _)
    }.getOrElse(Map.empty)
  }

  // not interesting akka streams
  def wc2(): Map[String, Int] = {
    val sink =
      Sink.fold[Map[String, Int], String](Map.empty)((acc, w) =>
        acc.updatedWith(w)({
          case Some(count) => Some(count + 1)
          case None        => Some(1)
        })
      )
    val future = source.runWith(sink)
    Await.result(future, Duration.Inf)
  }

  // mean to map-reduce, but too many substreams
  def wc3(): Map[String, Int] = {
    val flow =
      Flow[String]
        .groupBy(100000, identity)
        .map(w => (w, 1))
        .reduce((acc, wc) => (acc._1, acc._2 + wc._2))
        .mergeSubstreams
    val sink =
      Sink.fold[Map[String, Int], (String, Int)](Map.empty)((acc, wc) =>
        acc.updated(wc._1, wc._2)
      )
    val future = source.via(flow).runWith(sink)
    Await.result(future, Duration.Inf)
  }

  // just enough substreams to map-reduce
  def wc4(): Map[String, Int] = {
    val flow =
      Flow[String]
      // hashCode can be negative
        .groupBy(100, w => (w.hashCode & 0xfffffff) % 100)
        .fold(Map.empty[String, Int])((acc, w) =>
          acc.updatedWith(w) {
            case Some(count) => Some(count + 1)
            case None        => Some(1)
          }
        )
        .mergeSubstreams
    val sink = Sink.fold[Map[String, Int], Map[String, Int]](Map.empty)(_ ++ _)
    val future = source.via(flow).runWith(sink)
    Await.result(future, Duration.Inf)
  }

  def source: Source[String, Future[IOResult]] = {
    FileIO
      .fromPath(Paths.get(filepath))
      .map(_.utf8String)
      .mapConcat(_.split("\\W+"))
      .map(_.toLowerCase)
  }
}
