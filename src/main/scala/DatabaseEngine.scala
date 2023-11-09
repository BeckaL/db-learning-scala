import cats.data.EitherT
import cats.effect.IO
import cats.implicits.*
import model.{BinaryStringLengthExceeded, DatabaseException, DatabaseMetadata, FoundUnexpectedKeyAtOffset, KeyNotFoundInIndices, LogFile, NotEnoughLogFilesToCompress}

import java.nio.file.{Path, Paths}
import java.util.UUID

def storeKeyValue(key: String, value: String, engine: DatabaseMetadata): IO[Either[DatabaseException, DatabaseMetadata]] =
  (for {
    string       <- EitherT.fromEither[IO](getStringToWrite(key, value))
    existingSize <- EitherT.right(getExistingFileSize(engine.logFiles.head.path))
    hasCapacity = (string.length + existingSize) <= engine.fileLimit
    updatedEngine <- if (hasCapacity) EitherT.right(IO.pure(engine)) else EitherT.right(createNewLogFile(engine))
    i             <- EitherT.right(writeToFile(string, updatedEngine.logFiles.head.path))
  } yield updatedEngine.withUpdatedLogFileIndex(updatedEngine.logFiles.head.index.updated(key, i))).value

def getFromKey(key: String, metadata: DatabaseMetadata): IO[Either[DatabaseException, String]] =
  findIndexFromLogFiles(key, metadata.logFiles) match
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
  if (dbMetadata.logFiles.size < 3) {
    IO.pure(Left(NotEnoughLogFilesToCompress(dbMetadata.logFiles.length)))
  } else {
    val olderFile = dbMetadata.logFiles.last
    val newerFile = dbMetadata.logFiles.dropRight(1).last

    (for {
      newFile        <- EitherT.right(createNewFile(fileNameUpdater(dbMetadata)))
      updatedLogFile <- writeMapToNewLogFile(olderFile, newerFile, newFile)
    } yield dbMetadata.copy(logFiles = dbMetadata.logFiles.dropRight(2) :+ updatedLogFile))
      .value
      .flatTap(_ => deleteFile(olderFile.path))
      .flatTap(_ => deleteFile(newerFile.path))
  }
}

private def writeMapToNewLogFile(
  olderFile: LogFile,
  newerFile: LogFile,
  newLogFileName: Path
): EitherT[IO, DatabaseException, LogFile] =
  // Here we merge two maps with ++. The second map takes precedence in the case of duplicate keys i.e. newer takes precedence
  val keysToFilePaths = olderFile.index.view.mapValues((_, olderFile.path)).toMap ++
    newerFile.index.view.mapValues((_, newerFile.path))

  EitherT.apply(keysToFilePaths
    .toList
    .traverse { case (key, (offset, path)) => writeToNewIndex(key, path, offset, newLogFileName) }
    .map(_.sequence.map(listMap => LogFile(newLogFileName, listMap.toMap))))

def newLogName(dbMetadata: DatabaseMetadata) = Paths.get(dbMetadata.path.toString + "/" + UUID.randomUUID().toString + ".txt")

private def writeToNewIndex(
  k: String,
  pathToReadFrom: Path,
  offsetToReadFrom: Long,
  newIndex: Path
): IO[Either[DatabaseException, (String, Long)]] = {
  (for {
    value <- EitherT.apply(readFromFile(offsetToReadFrom, pathToReadFrom)) // TODO no check here for corruption
    string: String <- EitherT.fromEither[IO](getStringToWrite(k, value._2))
    index          <- EitherT.right(writeToFile(string, newIndex))
  } yield (k, index)).value
}

private def toPaddedBinaryString(i: Int): Either[DatabaseException, String] =
  val binString = i.toBinaryString
  if (binString.length > 8)
    Left(BinaryStringLengthExceeded(binString.length))
  else
    Right("0" * (8 - binString.length) + binString)
