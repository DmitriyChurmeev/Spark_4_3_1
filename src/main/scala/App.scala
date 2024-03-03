import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}
import scala.util.Try

object App extends Context {

  override val appName: String = "Spark_4_3_1"
  val formatter = new SimpleDateFormat("E", Locale.US)

  def main(args: Array[String]) = {

    val sc = spark.sparkContext


    val allLogDataRdd = sc.textFile("src/main/resources/logs_data_short.csv")
    val allRowsCount = allLogDataRdd.count()
    val actualLogDataRdd = allLogDataRdd
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
      .map(line => line.split(","))
      .map(values => buildLogIndo(values))
      .filter(_.isSuccess)
      .map(_.get)


    println("---Rows statistics---")
    println(s"All rows: ${allRowsCount}")
    println(s"Actual rows: ${actualLogDataRdd.count()} ")
    println(s"Failed rows: ${allRowsCount - actualLogDataRdd.count()} ")

    println("---Count response---")
    val aggregateLogIndoByResponse = actualLogDataRdd
      .groupBy(_.response)
      .aggregateByKey(0)((count, logInfos) => count + logInfos.size, (_, _) => 0)

    aggregateLogIndoByResponse
      .foreach(log => println(s"Response: ${log._1} Count: ${log._2}"))


    println("---Bytes statistics---")
    val maxBytes = actualLogDataRdd.sortBy(_.bytes, false).take(1)(0).bytes
    val minBytes = actualLogDataRdd.sortBy(_.bytes, true).take(1)(0).bytes
    val sumBytes = actualLogDataRdd.map(_.bytes).reduce(_ + _)

    println(s"Max bytes: $maxBytes")
    println(s"Min bytes: $minBytes")
    println(s"Sum bytes: $sumBytes")
    println(s"Avg bytes: ${sumBytes / actualLogDataRdd.count().toDouble}")

    val countHost = actualLogDataRdd.groupBy(_.host).count()

    println(s"Count of hosts: ${countHost}")

    val topHosts = actualLogDataRdd
      .groupBy(_.host)
      .aggregateByKey(0)((count, logInfos) => count + logInfos.size, (_, _) => 0)
      .sortBy(_._2, true)
      .take(3)

    println("---Top 3 of hosts---")
    topHosts.map {
        case (host, list) => HostVolume(
          host,
          list / (actualLogDataRdd.count() / 100).toDouble
        )
      }
      .foreach(host => println(s"Host: ${host.host} - ${host.count}%"))

    val groupByDay = actualLogDataRdd
      .filter(_.response.equals(404))
      .groupBy(row => timestampToDayOfWeek(row.time, formatter))
      .aggregateByKey(0)((count, logInfos) => count + logInfos.size, (_, _) => 0)
      .sortBy(_._2, false)
      .take(3)

    println("---Top 3 of day's of week for 404 status---")
    groupByDay.foreach(row => println(s"${row._1} : ${row._2}"))
  }

  def timestampToDayOfWeek(timestamp: Long, formatter: SimpleDateFormat): String = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(timestamp * 1000)
    formatter.format(calendar.getTime)
  }

  def buildLogIndo(row: Array[String]): Try[LogInfo] = Try {
    LogInfo(
      row(0).toInt,
      row(1),
      row(2).toInt,
      row(3),
      row(4),
      row(5).toInt,
      row(6).toInt)
  }

}




