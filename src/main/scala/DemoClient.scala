import java.net.InetAddress
import java.util
import java.util.UUID
import java.util.logging.Logger

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util._
import scala.util.control.NonFatal

class RecordProcessorFactory extends IRecordProcessorFactory {

  override def createProcessor: IRecordProcessor = new IRecordProcessor {

    private val log = Logger.getLogger("RecordProcessor")

    private var shardId: String = _
    private var nextCheckpointTimeMillis: Long = _

    private val MaxTries = 3
    private val DurationBetweenAttempts = 1000L
    private val CheckpointIntervalInMillis = 1000L

    override def initialize(shardId: String): Unit = {
      this.shardId = shardId
    }

    override def shutdown(check: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(check)
      }
    }

    override def processRecords(records: util.List[Record], check: IRecordProcessorCheckpointer): Unit = {

      records.asScala foreach { record =>
        try {
          val event = Json.parse(new String(record.getData.array()))
          log.info(s"Event: ${Json.prettyPrint(event)}")
        } catch {
          case NonFatal(e) =>
            log.severe(s"Exception: $e")
        }
      }

      if (System.currentTimeMillis() > nextCheckpointTimeMillis) {
        checkpoint(check)
        nextCheckpointTimeMillis = System.currentTimeMillis() + CheckpointIntervalInMillis
      }
    }

    /**
     * Generic method to attempt function up to number of tries
     *
     * @param maxTries                number of retries
     * @param fn                      function to execute
     * @return
     */
    private def retryUpTo(maxTries: Int = MaxTries)
                              (fn: => Any): Try[Any] = {
      @tailrec
      def retry(attempt: Int): Try[Any] = {
        Try {
          fn
        } match {
          case x: Success[_] => x
          case Failure(e) if attempt < maxTries =>
            Thread.sleep(DurationBetweenAttempts)
            retry(attempt + 1)
          case x: Failure[_] => x
        }
      }

      retry(1)
    }


    private[this] def checkpoint(check: IRecordProcessorCheckpointer): Unit = {
      retryUpTo(MaxTries) {
        check.checkpoint()
      }
    }
  }
}

object DemoClient extends App {

  val Endpoint = "https://kinesis.us-east-1.amazonaws.com"
  val AppName = "calatrava-demo-client"
  import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

  val orgsWorkerThread = new Thread(createWorker("updates-organisations"))
  val alertsWorkerThread = new Thread(createWorker("updates-alerts"))

  orgsWorkerThread.start()
  alertsWorkerThread.start()

  orgsWorkerThread.join()
  alertsWorkerThread.join()

  private[this] def createWorker(streamName: String): Worker = {
    val workerId = s"${InetAddress.getLocalHost.getCanonicalHostName}:${UUID.randomUUID()}"

    new Worker(
      new RecordProcessorFactory(),
      new KinesisClientLibConfiguration(
        s"$AppName.$streamName",
        streamName,
        new DefaultAWSCredentialsProviderChain,
        workerId
      ).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON),
      new NullMetricsFactory
    )
  }
}
