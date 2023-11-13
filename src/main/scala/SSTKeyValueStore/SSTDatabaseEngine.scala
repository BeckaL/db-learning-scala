package SSTKeyValueStore

import cats.effect.IO
import model.{DatabaseException, KeyNotFoundInIndices, LogFile}
import shared.{readFromFile, scanFileForKey}

import scala.collection.immutable.TreeMap

def write(metadata: SSTDatabaseMetadata, key: String, value: String): TreeMap[String, String] =
  metadata.memTable.updated(key, value)

def read(metadata: SSTDatabaseMetadata, key: String): IO[Either[DatabaseException, String]] =
  metadata.memTable.get(key) match {
    case Some(value) => IO.pure(Right(value))
    case None        => attemptToReadFromLogFiles(metadata.logFiles, key)
  }

private def attemptToReadFromLogFiles(logFiles: List[LogFile], key: String): IO[Either[DatabaseException, String]] =
  logFiles match
    case firstLogFile :: others => firstLogFile.index.get(key) match
        case Some(offset) => readFromFile(offset, firstLogFile.path).map(_.map(_._2))
        case None =>
          if (firstLogFile.index.nonEmpty)
            findOffsetToScan(firstLogFile.index, key) match
              case (startOffset, Some(endOffset)) =>
                scanFileForKey(startOffset, endOffset, firstLogFile.path, key).flatMap {
                  case Right(value)                  => IO.pure(Right(value))
                  case Left(KeyNotFoundInIndices(_)) => attemptToReadFromLogFiles(others, key)
                  case Left(otherException)          => ???
                }
              case _ => ???
          else attemptToReadFromLogFiles(others, key)
    case Nil => IO.pure(Left(KeyNotFoundInIndices(key)))

private def findOffsetToScan(index: Map[String, Long], keyToSearchFor: String): (Long, Option[Long]) =
  val sortedKeys = index.keys.toList.sorted
  val maybeNearestKeyAndNext =
    sortedKeys.dropRight(1).zip(sortedKeys.tail).find((key, next) => keyToSearchFor > key && keyToSearchFor < next)
  maybeNearestKeyAndNext match
    case Some((key, nextKey)) => (index(key), Some(index(nextKey)))
    case None                 => (index(sortedKeys.last), None)
