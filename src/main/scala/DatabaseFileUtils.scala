import cats.effect.IO
import model.{DatabaseMetadata, LogFile}

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.util.Try

val FIRST_LOG_FILE = "logFile1.txt"
def createDatabaseEngine(locationPrefix: String = "./src/main/resources", name: String): IO[DatabaseMetadata] =
  val directoryPathString = locationPrefix + "/" + name

  for {
    directoryPath <- tryIO(Paths.get(directoryPathString))
    filePath      <- tryIO(Paths.get(directoryPathString + "/" + FIRST_LOG_FILE))
    path          <- tryIO(Files.createDirectory(directoryPath))
    filePath      <- tryIO(Files.createFile(filePath))
  } yield DatabaseMetadata(directoryPath, List(LogFile(FIRST_LOG_FILE, Map())))

def writeToFile(stringToWrite: String, location: Path): IO[Long] =
  for {
    index <- tryIO(Files.size(location))
    file <- tryIO(Files.writeString(location, stringToWrite, StandardOpenOption.APPEND))
  } yield index

private def tryIO[T](f: T): IO[T] = IO.fromTry(Try(f))

