package akkaactors

import akka.actor.Scheduler
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.pattern.{RetrySupport, pipe}
import akka.util.Timeout
import akka.{actor => classic}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

object ClassicAskTyped extends App {
  val system: classic.ActorSystem = classic.ActorSystem("ClassicAskTyped")
  val typedSystem: ActorSystem[Nothing] = system.toTyped
  val ponger = system.spawn(Ponger(), "Ponger")
  val pinger =
    system.actorOf(classic.Props(new Pinger(ponger.toClassic)), "Pinger")

  class Pinger(ponger: classic.ActorRef)
      extends classic.Actor
      with classic.ActorLogging
      with RetrySupport {

    override def preStart(): Unit = {
      super.preStart()
      self ! Pinger.Initialize
    }

    override def receive: Receive = {
      case Pinger.Initialize =>
        import akka.pattern.extended.ask
        implicit val timeout: Timeout = Timeout(1.second)
        implicit val scheduler: Scheduler = context.system.scheduler
        import akka.actor.typed.scaladsl.adapter._
        implicit val ec: ExecutionContext = context.dispatcher
        retry(
          () => {
            val id = (new Random).nextInt(1000)
            log.info(s"Ping id=$id!")
            ponger.ask(ref => Ponger.Ping(id, ref)).mapTo[Ponger.Pong]
          },
          10,
          1.second
        ).pipeTo(self)

      case Ponger.Pong(id, v) =>
        log.info(s"Got a Pong id=$id, v=$v!")
        log.info("=========================")
    }
  }

  object Pinger {
    case object Initialize
  }

  object Ponger {

    def apply(): Behavior[Command] = {
      Behaviors.receive { (context, message) =>
        message match {
          case Ping(id, replyTo) =>
            context.log.info(s"Processing Ping id=$id!")
            replyTo ! Pong(id, 0)
            Behaviors.same
        }
      }
    }

    sealed trait Command

    sealed trait Reply

    case class Ping(id: Int, replyTo: ActorRef[Reply]) extends Command

    case class Pong(id: Int, v: Int) extends Reply

    private case class Set(v: Int) extends Command
  }
}

object ClassicAskClassic extends App {
  val system: classic.ActorSystem = classic.ActorSystem("ClassicAskClassic")
  val ponger = system.actorOf(classic.Props(new Ponger()), "Ponger")
  val pinger =
    system.actorOf(classic.Props(new Pinger(ponger)), "Pinger")

  class Pinger(ponger: classic.ActorRef)
      extends classic.Actor
      with classic.ActorLogging
      with RetrySupport {

    override def preStart(): Unit = {
      super.preStart()
      self ! Pinger.Initialize
    }

    override def receive: Receive = {
      case Pinger.Initialize =>
        import akka.pattern.ask
        implicit val timeout: Timeout = Timeout(1.second)
        implicit val scheduler: Scheduler = context.system.scheduler
        implicit val ec: ExecutionContext = context.dispatcher
        retry(
          () => {
            val id = (new Random).nextInt(1000)
            log.info(s"Ping id=$id!")
            ponger.ask(Ponger.Ping(id)).mapTo[Ponger.Pong]
          },
          10,
          1.second
        ).pipeTo(self)

      case Ponger.Pong(id, v) =>
        log.info(s"Got a Pong id=$id, v=$v!")
        log.info("=========================")
    }
  }

  class Ponger extends classic.Actor with classic.ActorLogging {
    override def receive: Receive = {
      case Ponger.Ping(id) =>
        sender() ! Ponger.Pong(id, 0)
    }
  }

  object Pinger {
    case object Initialize
  }

  object Ponger {
    case class Ping(id: Int)
    case class Pong(id: Int, v: Int)
  }
}
