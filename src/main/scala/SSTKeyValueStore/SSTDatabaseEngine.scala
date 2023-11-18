package SSTKeyValueStore

import cats.data.EitherT
import cats.effect.IO
import cats.implicits.*
import model.{DatabaseException, KeyNotFoundInIndices, LogFile, SSTDatabaseMetadata}
import shared.{createNewFile, getFileSize, getStringToWrite, getStringToWriteUnsafe, readFromFile, scanFileForKey, writeMemtableToFile, writeToFile}

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.collection.immutable.TreeMap

def write(
  metadata: SSTDatabaseMetadata,
  key: String,
  value: String,
  fileNameUpdater: SSTDatabaseMetadata => Path = newLogName
): IO[SSTDatabaseMetadata] =
  if (metadata.memTable.size > 100 && !metadata.memTable.contains(key))
    compressMemtableAndWriteToSSTFile(metadata, fileNameUpdater)
      .map(metadataAfterCompress => metadataAfterCompress.withUpdatedKeyValue(key, value))
  else
    IO.pure(metadata.withUpdatedKeyValue(key, value))

def read(metadata: SSTDatabaseMetadata, key: String): IO[Either[DatabaseException, String]] =
  metadata.memTable.get(key) match {
    case Some(value) => IO.pure(Right(value))
    case None        => attemptToReadFromLogFiles(metadata.logFiles, key)
  }

protected def compressMemtableAndWriteToSSTFile(
  metadata: SSTDatabaseMetadata,
  newFileNamer: SSTDatabaseMetadata => Path
): IO[SSTDatabaseMetadata] =
  for {
    newFilePath <- createNewFile(newFileNamer(metadata))
    index       <- writeMemtableToFile(metadata.memTable.toList, newFilePath)
    newLogFile = LogFile(newFilePath, index)
  } yield SSTDatabaseMetadata(metadata.path, TreeMap(), newLogFile +: metadata.logFiles)

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

private def newLogName(dbMetadata: SSTDatabaseMetadata) =
  Paths.get(dbMetadata.path.toString + "/" + UUID.randomUUID().toString + ".txt")
