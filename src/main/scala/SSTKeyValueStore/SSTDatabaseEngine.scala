package SSTKeyValueStore

import cats.data.EitherT
import cats.effect.IO
import cats.implicits.*
import model.{DatabaseException, KeyNotFoundInIndices, LogFile}
import shared.{createNewFile, getFileSize, getStringToWrite, readFromFile, scanFileForKey, writeToFile}

import java.nio.file.Path
import scala.collection.immutable.TreeMap

def write(metadata: SSTDatabaseMetadata, key: String, value: String): TreeMap[String, String] =
  metadata.memTable.updated(key, value)

def read(metadata: SSTDatabaseMetadata, key: String): IO[Either[DatabaseException, String]] =
  metadata.memTable.get(key) match {
    case Some(value) => IO.pure(Right(value))
    case None        => attemptToReadFromLogFiles(metadata.logFiles, key)
  }

def compressAndWriteToSSTFile(
  metadata: SSTDatabaseMetadata,
  newFileNamer: SSTDatabaseMetadata => Path
): IO[Either[DatabaseException, SSTDatabaseMetadata]] = {
  var indexForNewLogFile = Map()
  var count              = 0
  (for {
    newFilePath                   <- EitherT.right[DatabaseException](createNewFile(newFileNamer(metadata)))
    memtableListWithStringToWrite <- EitherT.fromEither[IO](getKeyValueAndStringToWrite(metadata))
    index                         <- EitherT.right(writeMemtableToFile(memtableListWithStringToWrite, newFilePath))
    newLogFile = LogFile(newFilePath, index)
  } yield SSTDatabaseMetadata(TreeMap(), newLogFile +: metadata.logFiles)).value
}

private def getKeyValueAndStringToWrite(metadata: SSTDatabaseMetadata): Either[DatabaseException, List[(String, String, String)]] =
  metadata.memTable.toList.traverse((key, value) => getStringToWrite(key, value).map(s => (key, value, s)))

private def writeMemtableToFile(
  memtableList: Seq[(String, String, String)],
  path: Path,
  currentIndex: Map[String, Long] = Map(),
  countOfValuesWritten: Int = 0
): IO[Map[String, Long]] =
  memtableList match {
    case (nextKey, nextValue, stringToWrite) :: remainingKeysAndValues =>
      writeToFile(stringToWrite, location = path).map(offset =>
        if (countOfValuesWritten % 10 == 0) currentIndex.updated(nextKey, offset) else currentIndex
      ).flatMap(updatedIndex =>
        writeMemtableToFile(remainingKeysAndValues, path, updatedIndex, countOfValuesWritten + 1)
      )
    case Nil => IO.pure(currentIndex)
  }

private def attemptToReadFromLogFiles(logFiles: List[LogFile], key: String): IO[Either[DatabaseException, String]] =
  logFiles match
    case firstLogFile :: others => firstLogFile.index.get(key) match
        case Some(offset) => readFromFile(offset, firstLogFile.path).map(_.map(_._2))
        case None =>
          if (firstLogFile.index.nonEmpty)
            val (startOffset, maybeEndOffset) = findOffsetToScan(firstLogFile.index, key)
            val endOffsetIO = maybeEndOffset match {
              case None         => getFileSize(firstLogFile.path)
              case Some(offset) => IO.pure(offset)
            }
            endOffsetIO.flatMap(endOffset =>
              scanFileForKey(startOffset, endOffset, firstLogFile.path, key).flatMap {
                case Right(value)                  => IO.pure(Right(value))
                case Left(KeyNotFoundInIndices(_)) => attemptToReadFromLogFiles(others, key)
                case Left(otherException)          => IO.pure(Left(otherException))
              }
            )
          else {
            attemptToReadFromLogFiles(others, key)
          }
    case Nil => IO.pure(Left(KeyNotFoundInIndices(key)))

private def findOffsetToScan(index: Map[String, Long], keyToSearchFor: String): (Long, Option[Long]) =
  val sortedKeys = index.keys.toList.sorted
  val maybeNearestKeyAndNext =
    sortedKeys.dropRight(1).zip(sortedKeys.tail).find((key, next) => keyToSearchFor > key && keyToSearchFor < next)
  maybeNearestKeyAndNext match
    case Some((key, nextKey)) => (index(key), Some(index(nextKey)))
    case None                 => (index(sortedKeys.last), None)
