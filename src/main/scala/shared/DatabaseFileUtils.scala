package shared

import SimpleKeyValueStore.SimpleDatabaseMetadata
import cats.data.EitherT
import cats.effect.IO
import model.{BinaryStringLengthExceeded, DatabaseException, KeyNotFoundInIndices, LogFile, ReadTooSmallValue, UnparseableBinaryString}

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.util.{Failure, Success, Try}

def createDatabaseEngine(
  locationPrefix: String = "./src/main/resources",
  name: String,
  logFileSizeLimit: Long
): IO[SimpleDatabaseMetadata] =
  val directoryPathString = locationPrefix + "/" + name

  for {
    directoryPath <- tryIO(Paths.get(directoryPathString))
    initialMetadata = SimpleDatabaseMetadata(directoryPath, List(), logFileSizeLimit)
    _               <- tryIO(Files.createDirectory(directoryPath))
    updatedMetadata <- createNewLogFile(initialMetadata)
  } yield updatedMetadata

def createNewLogFile(databaseMetadata: SimpleDatabaseMetadata): IO[SimpleDatabaseMetadata] =
  val name = s"logFile${databaseMetadata.logFiles.length + 1}.txt"
  for {
    filePath <- tryIO(Paths.get(databaseMetadata.path.toString + "/" + name))
    _        <- createNewFile(filePath)
  } yield databaseMetadata.copy(logFiles = LogFile(filePath, Map()) +: databaseMetadata.logFiles)

def createNewFile(path: Path): IO[Path] = tryIO(Files.createFile(path)) //TODO make this the standard way of creating new name

def writeToFile(stringToWrite: String, location: Path): IO[Long] =
  for {
    index <- getExistingFileSize(location)
    file  <- tryIO(Files.writeString(location, stringToWrite, StandardOpenOption.APPEND))
  } yield index

def getExistingFileSize(location: Path): IO[Long] = tryIO(Files.size(location))

def deleteFile(location: Path): IO[Unit] = tryIO(Files.delete(location)).void

def getStringToWrite(key: String, value: String): Either[DatabaseException, String] =
  for {
    keySize   <- toPaddedBinaryString(key.length)
    valueSize <- toPaddedBinaryString(value.length)
  } yield keySize + key + valueSize + value

def readFromFile(offset: Long, location: Path): IO[Either[DatabaseException, (String, String)]] =
  readFromFileReturningPosition(offset, location).map(_.map((key, value, _) => (key, value)))

private def readFromFileReturningPosition(offset: Long, location: Path): IO[Either[DatabaseException, (String, String, Long)]] = {
  (for {
    fileChannel <- EitherT.right(tryIO(FileChannel.open(location).position(offset)))
    keySize     <- readBinaryIntegerFromFile(fileChannel)
    key         <- EitherT.apply(tryIO(readChunkFromFile(keySize, fileChannel)))
    valueSize   <- readBinaryIntegerFromFile(fileChannel)
    value       <- EitherT.apply(tryIO(readChunkFromFile(valueSize, fileChannel)))
    position = fileChannel.position()
    _        = fileChannel.close()
  } yield (key, value, position)).value
}

def scanFileForKey(startOffset: Long, until: Long, location: Path, keyToFind: String): IO[Either[DatabaseException, String]] =
  readFromFileReturningPosition(startOffset, location).flatMap {
    case Right((key, value, _)) if key == keyToFind         => IO.pure(Right(value))
    case Right((_, _, endPosition)) if endPosition >= until => IO.pure(Left(KeyNotFoundInIndices(keyToFind)))
    case Right((_, _, endPosition))                         => scanFileForKey(endPosition, until, location, keyToFind)
    case Left(databaseException)                            => IO.pure(Left(databaseException))
  }

private def readBinaryIntegerFromFile(fileChannel: FileChannel): EitherT[IO, DatabaseException, Integer] =
  for {
    binaryString <- EitherT.apply(tryIO(readChunkFromFile(8, fileChannel)))
    integer <- Try(Integer.parseInt(binaryString, 2)) match {
      case Success(int) => EitherT.right[DatabaseException](IO.pure(int))
      case Failure(_)   => EitherT.fromEither[IO](Left(UnparseableBinaryString(binaryString)))
    }
  } yield integer

//This should be EitherT of IO-ified
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

private def toPaddedBinaryString(i: Int): Either[DatabaseException, String] =
  val binString = i.toBinaryString
  if (binString.length > 8)
    Left(BinaryStringLengthExceeded(binString.length))
  else
    Right("0" * (8 - binString.length) + binString)
