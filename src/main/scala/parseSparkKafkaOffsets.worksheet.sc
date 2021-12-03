import cats.kernel.Semigroup
import io.circe._
import io.circe.parser._
import cats.syntax.either._
import cats.syntax.traverse._
import cats.instances.either._
import cats.instances.list._
import io.circe.syntax._
import cats.syntax.apply._

case class TopicName(value: String)
object TopicName {
  implicit val topicEncoder: Encoder[TopicName] =
    Encoder[String].contramap(_.value)
  implicit val topicKeyEncoder: KeyEncoder[TopicName] =
    KeyEncoder.encodeKeyString.contramap(_.value)
}

case class Partition(value: Int)
object Partition {
  implicit val partitionEncoder: Encoder[Partition] =
    Encoder[Int].contramap(_.value)
  implicit val partitionKeyEncoder: KeyEncoder[Partition] =
    KeyEncoder.encodeKeyInt.contramap(_.value)
}

case class Offset(value: Long) {
  def asStartDelta: OffsetDelta = OffsetDelta(-value)
  def asEndDelta: OffsetDelta = OffsetDelta(value)
}
object Offset {
  implicit val offsetEncoder: Encoder[Offset] = Encoder[Long].contramap(_.value)
}

case class OffsetDelta(value: Long)
object OffsetDelta {
  implicit val offsetSemigroup: Semigroup[OffsetDelta] =
    Semigroup.instance((x, y) => OffsetDelta(x.value + y.value))
  implicit val offsetEncoder: Encoder[OffsetDelta] =
    Encoder[Long].contramap(_.value)
}

def parseTopic(
    topicName: String,
    topic: Map[String, Json]
): Either[Throwable, (TopicName, Map[Partition, Offset])] = {
  topic.toList
    .traverse { case (p, f) =>
      for {
        partition <- Either.catchNonFatal(p.toInt)
        offset <- f.as[Long]
      } yield Partition(partition) -> Offset(offset)
    }
    .map(list => TopicName(topicName) -> list.toMap)
}

def parseFile(
    s: String
): Either[Throwable, Map[TopicName, Map[Partition, Offset]]] = {
  val lines = s.split("\n", 3).toList
  for {
    _ <- Either.catchNonFatal(require(lines.size == 3, "not 3 lines"))
    _ <- Either.catchNonFatal(require(lines.head == "v1", "no v1"))
    root <- parse(lines(2))
    obj <- root.as[Map[String, Json]]
    v <- obj.toMap.toList
      .traverse { case (topicName, v) =>
        for {
          innerObj <- v.as[JsonObject]
          v <- parseTopic(topicName, innerObj.toMap)
        } yield (v)
      }
  } yield {
    v.toMap
  }
}

val offset97 = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1638036945077,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"contacthistory-raw.topic":{"17":4996388,"8":4996183,"11":4997979,"2":19395006,"5":4998481,"14":4996330,"13":4996233,"4":4996291,"16":4998389,"7":4996386,"1":19398457,"10":4998474,"19":4995916,"18":4996140,"9":4996366,"3":4995758,"12":4996394,"15":4998458,"6":4998380,"0":19396607}}"""

val offset98 = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1638036946002,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"contacthistory-raw.topic":{"17":4996388,"8":4996183,"11":4997979,"2":19395006,"5":4998481,"14":4996330,"13":4996233,"4":4996291,"16":4998389,"7":4996386,"1":19398458,"10":4998474,"19":4995917,"18":4996140,"9":4996366,"3":4995758,"12":4996394,"15":4998458,"6":4998381,"0":19396608}}"""

val offset99 = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1638036947004,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"contacthistory-raw.topic":{"17":4996388,"8":4996183,"11":4997979,"2":19395006,"5":4998481,"14":4996330,"13":4996233,"4":4996291,"16":4998389,"7":4996386,"1":19398458,"10":4998474,"19":4995917,"18":4996140,"9":4996366,"3":4995758,"12":4996394,"15":4998458,"6":4998381,"0":19396608}}"""

val offset100 = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1638103629541,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"contacthistory-raw.topic":{"17":4996388,"8":4996183,"11":4997979,"2":19395006,"5":4998481,"14":4996330,"13":4996233,"4":4996291,"16":4998389,"7":4996386,"1":19398458,"10":4998474,"19":4995917,"18":4996140,"9":4996366,"3":4995758,"12":4996394,"15":4998458,"6":4998381,"0":19396608}}"""

val offset101 = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1638109214199,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
{"contacthistory-raw.topic":{"17":2846180,"8":2845960,"11":4997979,"2":19395006,"5":2846278,"14":4996330,"13":2846032,"4":4996291,"16":2846220,"7":4996386,"1":19398458,"10":4998474,"19":4995917,"18":4996140,"9":4996366,"3":4995758,"12":4996394,"15":4998458,"6":4998381,"0":19396608}}"""

def readFile(fileName: String)(implicit codec: scala.io.Codec): Either[Throwable, String] = {
  Either.catchNonFatal(scala.io.Source.fromFile(fileName).getLines().mkString("\n"))
}

def increments(
    topics0: Map[TopicName, Map[Partition, OffsetDelta]],
    topics1: Map[TopicName, Map[Partition, OffsetDelta]]
): Map[TopicName, Map[Partition, OffsetDelta]] = {
  Semigroup.combine(topics1, topics0)
}

def whichAreGoingBack(
    res: Map[TopicName, Map[Partition, OffsetDelta]]
): Map[TopicName, Map[Partition, OffsetDelta]] =
  res
    .map { case (t, partitions) =>
      t -> partitions.filter { case (_, o) =>
        o.value < 0
      }
    }
    .filterNot(_._2.isEmpty)

def printEitherS[A](e: Either[A, String]): String = printEither(e)(identity)

def printEither[A, B](e: Either[A, B])(f: B => String): String =
  e.fold(_ => "⚠️ Ops! ⚠️", f)

def toDeltaStart(
    m: Map[TopicName, Map[Partition, Offset]]
): Map[TopicName, Map[Partition, OffsetDelta]] = {
  m.view.mapValues { v =>
    v.view.mapValues(_.asStartDelta).toMap
  }.toMap
}

def toDeltaEnd(
    m: Map[TopicName, Map[Partition, Offset]]
): Map[TopicName, Map[Partition, OffsetDelta]] = {
  m.view.mapValues { v =>
    v.view.mapValues(_.asEndDelta).toMap
  }.toMap
}

def logic(off0: String, off1: String) = {
  for {
    t0 <- parseFile(off0)
    t1 <- parseFile(off1)
  } yield {
    val inc = increments(toDeltaStart(t0), toDeltaEnd(t1))
    val goneBack = whichAreGoingBack(inc)
    if (goneBack.nonEmpty) {
      "[KO] is going back on topics and partitions\n" +
        goneBack.asJson.spaces2SortKeys + "\n" +
        t0.asJson.spaces2SortKeys + "\n" +
        t1.asJson.spaces2SortKeys
    } else {
      "[OK] is NOT going back"
    }
  }
}

printEitherS(logic(offset97, offset98))
printEitherS(logic(offset98, offset99))
printEitherS(logic(offset99, offset100))
printEitherS(logic(offset100, offset101))
