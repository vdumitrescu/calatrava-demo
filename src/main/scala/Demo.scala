import com.gilt.calatrava.kinesis.{CalatravaEventProcessor, BridgeConfiguration, BridgeWorkerFactory}
import com.gilt.calatrava.v0.models.ChangeEvent

object Demo extends App {

  class EventProcessor(name: String) extends CalatravaEventProcessor {
    override def processEvent(event: ChangeEvent): Boolean = {
      println(s"[$name] Event received: $event")
      true
    }

    override def processHeartBeat(): Unit = {
      println(s"[$name] Heartbeat received!")
    }
  }

  val ClientAppName = "calatrava-demo"
  if (args.length % 3 != 0) sys.error("Usage: demo [<stream> <bucket> <role>]*") else {

    val workers = args.grouped(3) map { trio =>
      val streamName = trio(0)
      val bucketName = trio(1)
      val roleArn = Some(trio(2))
      new Thread(
        new BridgeWorkerFactory(
          new EventProcessor(streamName),
          BridgeConfiguration(s"$ClientAppName-$streamName", streamName, bucketName, roleArn, None)
        ).instance()
      )
    }

    workers.foreach(_.start())
    workers.foreach(_.join())
  }
}
