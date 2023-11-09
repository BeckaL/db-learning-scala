import cats.data.EitherT
import cats.effect.IO
import model.{DatabaseException, DatabaseMetadata, LogFile, ReadTooSmallValue, UnparseableBinaryString}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.util.{Failure, Success, Try}

def createDatabaseEngine(locationPrefix: String = "./src/main/resources", name: String, logFileSizeLimit: Long): IO[DatabaseMetadata] =
  val directoryPathString = locationPrefix + "/" + name

  for {
    directoryPath <- tryIO(Paths.get(directoryPathString))
    initialMetadata = DatabaseMetadata(directoryPath, List(), logFileSizeLimit)
    _               <- tryIO(Files.createDirectory(directoryPath))
    updatedMetadata <- createNewLogFile(initialMetadata)
  } yield updatedMetadata

def createNewLogFile(databaseMetadata: DatabaseMetadata): IO[DatabaseMetadata] =
  val name = s"logFile${databaseMetadata.indices.length + 1}.txt"
  for {
    filePath <- tryIO(Paths.get(databaseMetadata.path.toString + "/" + name))
    _        <- createNewFile(filePath)
  } yield databaseMetadata.copy(indices = LogFile(filePath, Map()) +: databaseMetadata.indices)

def createNewFile(path: Path): IO[Path] = tryIO(Files.createFile(path))

def writeToFile(stringToWrite: String, location: Path): IO[Long] =
  for {
    index <- getExistingFileSize(location)
    file  <- tryIO(Files.writeString(location, stringToWrite, StandardOpenOption.APPEND))
  } yield index

def getExistingFileSize(location: Path): IO[Long] = tryIO(Files.size(location))

def deleteFile(location: Path): IO[Unit] = tryIO(Files.delete(location)).void

def getStringToWrite(key: String, value: String): Either[String, String] =
  for {
    keySize   <- toPaddedBinaryString(key.length)
    valueSize <- toPaddedBinaryString(value.length)
  } yield keySize + key + valueSize + value

def readFromFile(offset: Long, location: Path): IO[Either[DatabaseException, (String, String)]] = {
  (for {
    fileChannel <- EitherT.right(tryIO(FileChannel.open(location)))
    _ = fileChannel.position(offset)
    keySize   <- readBinaryIntegerFromFile(fileChannel)
    key       <- EitherT.apply(tryIO(readChunkFromFile(keySize, fileChannel)))
    valueSize <- readBinaryIntegerFromFile(fileChannel)
    value     <- EitherT.apply(tryIO(readChunkFromFile(valueSize, fileChannel)))
    _ = fileChannel.close()
  } yield (key, value)).value
}

private def readBinaryIntegerFromFile(fileChannel: FileChannel): EitherT[IO, DatabaseException, Integer] =
  for {
    binaryString <- EitherT.apply(tryIO(readChunkFromFile(8, fileChannel)))
    integer <- Try(Integer.parseInt(binaryString, 2)) match {
      case Success(int) => EitherT.right[DatabaseException](IO.pure(int))
      case Failure(_)   => EitherT.fromEither[IO](Left(UnparseableBinaryString(binaryString)))
    }
  } yield integer

private def readChunkFromFile(byteBufferSize: Int, fileChannel: FileChannel): Either[DatabaseException, String] =
  val buffer = ByteBuffer.allocate(byteBufferSize)
  while (buffer.hasRemaining && fileChannel.position() < fileChannel.size())
    fileChannel.read(buffer)
  buffer.position(0)
  buffer.limit(byteBufferSize)
  val filterBuffer = buffer.array().filter(e => e != 0)
  if (filterBuffer.length < byteBufferSize)
    Left(ReadTooSmallValue(byteBufferSize, filterBuffer.length))
  else
    Right(Charset.forName("UTF-8").decode(buffer).toString)

private def tryIO[T](f: T): IO[T] = IO.fromTry(Try(f))
