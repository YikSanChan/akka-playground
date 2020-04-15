package akkastreams

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DynamicStream extends App {
  val system = ActorSystem(Ticker(), "hello-world")
  implicit val ec: ExecutionContext = system.executionContext

  // Set the delay < 10 seconds to stop stream early
  // Otherwise, it stops together with the actor
  system.scheduler.scheduleOnce(5.seconds, () => system ! Ticker.StopStream)
}

object Ticker {

  sealed trait Command
  final case object StopStream extends Command
  final case object StopActor extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    implicit val mat: Materializer = Materializer(context.system)
    implicit val ec: ExecutionContext = context.system.executionContext

    // print ticks
    val (killSwitch, _) = Source
      .tick(1.second, 1.second, "tick")
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(println))(Keep.both)
      .run()

    // kill the actor in 10 seconds regardless
    context.system.scheduler
      .scheduleOnce(10.seconds, () => context.self ! Ticker.StopActor)

    Behaviors.receiveMessage {
      case StopStream =>
        context.log.info("Early Stop!")
        killSwitch.shutdown()
        Behaviors.stopped
      case StopActor =>
        context.log.info("Stop!")
        Behaviors.stopped
    }
  }
}
