import cats.data.EitherT
import cats.effect.IO
import model.{DatabaseException, DatabaseMetadata, LogFile, ReadTooSmallValue}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.util.Try

def createDatabaseEngine(locationPrefix: String = "./src/main/resources", name: String): IO[DatabaseMetadata] =
  val fileName            = "logFile1.txt"
  val directoryPathString = locationPrefix + "/" + name

  for {
    directoryPath <- tryIO(Paths.get(directoryPathString))
    filePath      <- tryIO(Paths.get(directoryPathString + "/" + fileName))
    path          <- tryIO(Files.createDirectory(directoryPath))
    _             <- tryIO(Files.createFile(filePath))
  } yield DatabaseMetadata(directoryPath, List(LogFile(filePath, Map())))

def writeToFile(stringToWrite: String, location: Path): IO[Long] =
  for {
    index <- tryIO(Files.size(location))
    file  <- tryIO(Files.writeString(location, stringToWrite, StandardOpenOption.APPEND))
  } yield index

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
    integer <- EitherT.right(tryIO(Integer.parseInt(binaryString, 2))) // TODO can make this a left not IO now
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
