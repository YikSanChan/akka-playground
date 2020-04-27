package akkastreams

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.RetrySupport
import akka.stream.scaladsl.{Flow, RetryFlow, Sink, Source}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object FlowWithRetrySupportApp extends App with RetrySupport {

  implicit val system: ActorSystem = ActorSystem("FlowWithRetrySupportApp")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val scheduler: Scheduler = system.scheduler

  val source = Source(1 to 10)
  val retryFlow = Flow[Int].mapAsync(3)(retryEchoFail50)

  source.via(retryFlow).runForeach(println).onComplete {
    case Success(_)  => println("flow completes successfully")
    case Failure(ex) => println(s"flow fails due to ${ex.getMessage}")
  }

  private def echoFail50(v: Int): Future[Int] = {
    if (ThreadLocalRandom.current().nextBoolean()) Future.successful(v)
    else {
      println(s"echo $v failure")
      Future.failed(new RuntimeException(s"echo $v failure"))
    }
  }

  private def retryEchoFail50(v: Int): Future[Int] = {
    retry(
      attempt = () => echoFail50(v),
      5,
      1.second
    )
  }
}

//echo 1 business failure
//echo 1 success
//echo 2 network failure
//echo 2 success
//echo 3 success
//echo 4 success
//echo 5 success
//echo 6 network failure
//echo 6 business failure
//echo 6 business failure
//echo 6 success
//echo 7 network failure
//echo 7 success
//echo 8 business failure
//echo 8 success
//echo 9 success
//echo 10 business failure
//echo 10 success
//all elements completes
object RetryFlowApp extends App {

  sealed trait MyException
  case class BusinessException(errMsg: String)
      extends RuntimeException(errMsg)
      with MyException
  case class NetworkException(errMsg: String)
      extends RuntimeException(errMsg)
      with MyException

  implicit val system: ActorSystem = ActorSystem("RetryFlowApp")
  implicit val ec: ExecutionContext = system.dispatcher

  val list = 1 to 10
  val source = Source(list)
  val flow = Flow[Int].mapAsync(3)(v =>
    flakyEchoAPI(v).transform {
      // handle future failure
      case Failure(ex) => Success(Left(NetworkException(ex.getMessage)))
      case success     => success
    }
  )
  val retryFlow = RetryFlow
    .withBackoff(
      minBackoff = 10.millis,
      maxBackoff = 5.seconds,
      randomFactor = 0d,
      maxRetries = 5,
      flow
    )(decideRetry = {
      case (in, Left(_)) => Some(in)
      case _             => None
    })

  source
    .via(retryFlow)
    .collect {
      case Right(x) => x
    }
    .runWith(Sink.seq)
    .onComplete {
      case Success(ns) =>
        val diff = list.toSet.diff(ns.toSet)
        if (diff.nonEmpty)
          println(s"run out of retries=$diff")
        else
          println("all elements completes")
      case Failure(ex) => println(s"flow completes with failure")
    }

  private def flakyEchoAPI(v: Int): Future[Either[MyException, Int]] = {
    val randomNext = ThreadLocalRandom.current().nextInt(3)
    if (randomNext == 0) {
      println(s"echo $v success")
      Future.successful(Right(v))
    } else if (randomNext == 1) {
      val errMsg = s"echo $v business failure"
      println(errMsg)
      Future.successful(Left(BusinessException(errMsg)))
    } else {
      val errMsg = s"echo $v network failure"
      println(errMsg)
      Future.failed(new Exception(errMsg))
    }
  }
}
