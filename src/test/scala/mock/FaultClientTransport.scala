package mock

import com.hypertino.hyperbus.model.{Body, Message, RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.typesafe.config.Config
import com.hypertino.hyperbus.transport.api._
import monix.eval.Task

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class FaultClientTransport(config: Config) extends ClientTransport {
  override def ask(message: RequestBase, responseDeserializer: ResponseBaseDeserializer): Task[ResponseBase] = {
    Task.raiseError(new RuntimeException("ask failed (test method)"))
  }

  override def publish(message: RequestBase): Task[PublishResult] = {
    if (FaultClientTransport.checkers.exists { checker ⇒
      checker.isDefinedAt(message) && checker(message)
    }) {
      Task.raiseError(new RuntimeException("publish failed (test method)"))
    }
    else {
      Task.eval {
        new PublishResult {
          override def sent: Option[Boolean] = None

          override def offset: Option[String] = None
        }
      }
    }
  }

  override def shutdown(duration: FiniteDuration): Task[Boolean] = Task.eval(true)
}

object FaultClientTransport {
  val checkers = mutable.ArrayBuffer[PartialFunction[RequestBase, Boolean]]()
}
