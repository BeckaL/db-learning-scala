package SSTKeyValueStore

import cats.effect.IO
import model.{DatabaseException, KeyNotFoundInIndices, LogFile}
import shared.readFromFile

import scala.collection.immutable.TreeMap

def write(metadata: SSTDatabaseMetadata, key: String, value: String): TreeMap[String, String] =
  metadata.memTable.updated(key, value)

def read(metadata: SSTDatabaseMetadata, key: String): IO[Either[DatabaseException, String]] =
  metadata.memTable.get(key) match {
    case Some(value) => IO.pure(Right(value))
    case None        => attemptToReadFromLogFiles(metadata.logFiles, key)
  }

private def attemptToReadFromLogFiles(logFiles: List[LogFile], key: String): IO[Either[DatabaseException, String]] =
  logFiles match {
    case firstLogFile :: others => firstLogFile.index.get(key) match {
      case None => attemptToReadFromLogFiles(others, key)
      case Some(offset) => readFromFile(offset, firstLogFile.path).map(_.map(_._2))
    }
    case Nil => IO.pure(Left(KeyNotFoundInIndices(key)))
  }
