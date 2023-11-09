import cats.data.EitherT
import cats.effect.IO
import cats.implicits.*
import model.{BinaryStringLengthExceeded, DatabaseException, DatabaseMetadata, FoundUnexpectedKeyAtOffset, KeyNotFoundInIndices, LogFile, NotEnoughLogFilesToCompress}

import java.nio.file.{Path, Paths}
import java.util.UUID

def storeKeyValue(key: String, value: String, engine: DatabaseMetadata): IO[Either[DatabaseException, DatabaseMetadata]] =
  (for {
    string       <- EitherT.fromEither[IO](getStringToWrite(key, value))
    existingSize <- EitherT.right(getExistingFileSize(engine.indices.head.path))
    hasCapacity = (string.length + existingSize) <= engine.fileLimit
    updatedEngine <- if (hasCapacity) EitherT.right(IO.pure(engine)) else EitherT.right(createNewLogFile(engine))
    i             <- EitherT.right(writeToFile(string, updatedEngine.indices.head.path))
  } yield updatedEngine.withUpdatedLogFileIndex(updatedEngine.indices.head.index.updated(key, i))).value

def getFromKey(key: String, metadata: DatabaseMetadata): IO[Either[DatabaseException, String]] =
  findIndexFromLogFiles(key, metadata.indices) match
    case Left(exception) => IO.pure(Left(exception))
    case Right((path, offset)) => readFromFile(offset, path).map {
        case Right((retrievedKey, value)) if retrievedKey == key => Right(value)
        case Right((otherKey, _)) =>
          Left(FoundUnexpectedKeyAtOffset(key, path, offset, otherKey))
        case Left(error) => Left(error)
      }

private def findIndexFromLogFiles(key: String, logFiles: List[LogFile]): Either[DatabaseException, (Path, Long)] =
  logFiles match
    case Nil => Left(KeyNotFoundInIndices(key))
    case firstLogFile :: others => firstLogFile.index.get(key) match
        case Some(i) => Right((firstLogFile.path, i))
        case None    => findIndexFromLogFiles(key, others)

def compress(
  dbMetadata: DatabaseMetadata,
  fileNameUpdater: DatabaseMetadata => Path = newLogName
): IO[Either[DatabaseException, DatabaseMetadata]] = {
  if (dbMetadata.indices.size < 3) {
    IO.pure(Left(NotEnoughLogFilesToCompress(dbMetadata.indices.length)))
  } else {
    val olderFile = dbMetadata.indices.last
    val newerFile = dbMetadata.indices.dropRight(1).last

    // Here we merge two maps with ++. The second map takes precedence in the case of duplicate keys i.e. newer takes precedence
    val keysToFilePaths = olderFile.index.view.mapValues((_, olderFile.path)).toMap ++
      newerFile.index.view.mapValues((_, newerFile.path))

    (for {
      newFile      <- EitherT.right(createNewFile(fileNameUpdater(dbMetadata)))
      updatedIndex <- EitherT.right(writeMapToNewIndex(newFile, keysToFilePaths, newFile))
    } yield dbMetadata.copy(indices = List(LogFile(newFile, updatedIndex))))
      .value
      .flatTap(_ => deleteFile(olderFile.path))
      .flatTap(_ => deleteFile(newerFile.path))
  }
}

private def writeMapToNewIndex(path: Path, keysToFilePaths: Map[String, (Long, Path)], newLogFileName: Path): IO[Map[String, Long]] =
  keysToFilePaths
    .toList
    .traverse { case (key, (offset, path)) => writeToNewIndex(key, path, offset, newLogFileName) }
    .map(_.toMap)

def newLogName(dbMetadata: DatabaseMetadata) = Paths.get(UUID.randomUUID().toString + ".txt")

private def writeToNewIndex(k: String, pathToReadFrom: Path, offsetToReadFrom: Long, newIndex: Path): IO[(String, Long)] = {
  (for {
    value <- EitherT.apply(readFromFile(offsetToReadFrom, pathToReadFrom)) // TODO no check here for corruption
    string: String <- EitherT.fromEither[IO](getStringToWrite(k, value._2))
    index          <- EitherT.right(writeToFile(string, newIndex))
  } yield (k, index)).value.map(_.getOrElse(throw new RuntimeException("uh oh")))
}

private def toPaddedBinaryString(i: Int): Either[DatabaseException, String] =
  val binString = i.toBinaryString
  if (binString.length > 8)
    Left(BinaryStringLengthExceeded(binString.length))
  else
    Right("0" * (8 - binString.length) + binString)
