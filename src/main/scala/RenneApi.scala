import com.typesafe.config.ConfigFactory
import scalaj.http.Http

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
object RenneApi extends App {

  // define the api url
  val config = ConfigFactory.load("application.conf")

  def fetchDataAndWriteToFile(): Unit = {
    val response = Http(config.getString("API.url")).asString

    // convert data to json
    val jsonString = response.body

    // create a file name based on the current timestamp
    val fileName = s"${System.currentTimeMillis()}.json"
    val filePath = Paths.get(config.getString("API.output"), fileName)

    // write data to file
    Files.createDirectories(filePath.getParent)
    val writer = new PrintWriter(filePath.toFile)
    writer.write(jsonString)
    writer.close()

     //todo: use logger instead of println
    println(s"Data written to file: $fileName")
  }

  //define a scheduler to fetch data every minute
  val duration = Duration(1, MINUTES)
  val scheduler = new java.util.Timer()

  scheduler.schedule(new java.util.TimerTask {
    def run() = {
      fetchDataAndWriteToFile()
    }
  }, 0, duration.toMillis)

  // Arrêter la planification après 6 minutes
  Thread.sleep(15 * 60 * 1000)
  scheduler.cancel()


}
