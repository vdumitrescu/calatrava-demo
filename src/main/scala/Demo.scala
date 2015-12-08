import com.gilt.calatrava.kinesis.{CalatravaEventProcessor, BridgeConfiguration, BridgeWorkerFactory}
import com.gilt.calatrava.v0.models.ChangeEvent

object Demo extends App {

  class EventProcessor extends CalatravaEventProcessor {
    override def processEvent(event: ChangeEvent): Boolean = {
      println(s"Received event: $event")
      true
    }

    override def processHeartBeat(): Unit = {
      println("Heartbeat received!")
    }
  }

  val ClientAppName = "calatrava-demo"
  if (args.length % 3 != 0) sys.error("Usage: demo [<stream> <bucket> <role>]*") else {

    val workers = args.grouped(3) map { trio =>
      new Thread(new BridgeWorkerFactory(new EventProcessor(), BridgeConfiguration(ClientAppName, trio(0), trio(1), Some(trio(2)), None)).instance())
    }

    workers.foreach(_.start())
    workers.foreach(_.join())
  }

}
