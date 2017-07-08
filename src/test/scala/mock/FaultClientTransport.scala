package mock

import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api._
import com.typesafe.config.Config
import monix.eval.Task
import scaldi.Injector

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class FaultClientTransport(config: Config, implicit val injector: Injector) extends ClientTransport {
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
