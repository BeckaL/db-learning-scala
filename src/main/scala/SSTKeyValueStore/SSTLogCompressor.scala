package SSTKeyValueStore

import cats.data.EitherT
import cats.effect.IO
import model.{DatabaseException, KeyValuePair, LogFile}

import java.nio.channels.FileChannel
import java.nio.file.Path
import shared.{createNewFile, getFileSize, getStringToWriteUnsafe, readKeyValueFromFileChannel, writeMemtableToFile}

import scala.collection.immutable.TreeMap
import scala.util.Try

def compress(newerFile: Path, olderFile: Path, newFilePath: Path): IO[Either[DatabaseException, Map[String, Long]]] = {
  val blah = for {
    filePath         <- createNewFile(newFilePath)
    newerFileChannel <- tryIO(FileChannel.open(newerFile))
    olderFileChannel <- tryIO(FileChannel.open(olderFile))
  } yield (filePath, newerFileChannel, olderFileChannel)

  blah.flatMap { case (filePath, newerFileChannel, olderFileChannel) =>
    (for {
      map          <- getTreeMapFromFiles(newerFileChannel, olderFileChannel)
      updatedIndex <- EitherT.right(writeMemtableToFile(map.toList, newFilePath))
    } yield updatedIndex).value
  }
}

private def getTreeMapFromFiles(
  newerFileChannel: FileChannel,
  olderFileChannel: FileChannel
): EitherT[IO, DatabaseException, TreeMap[String, String]] = {
  def go(treeMap: TreeMap[String, String]): EitherT[IO, DatabaseException, TreeMap[String, String]] = {
    val newerStartPosition = newerFileChannel.position()
    val olderStartPosition = olderFileChannel.position()

    if (newerStartPosition == newerFileChannel.size() && olderStartPosition == olderFileChannel.size()) {
      EitherT.pure(treeMap)
    } else {
      (for {
        maybeNewerKeyValue <- getMaybeKeyValue(newerFileChannel)
        maybeOlderKeyValue <- getMaybeKeyValue(olderFileChannel)
        (keyValue, resetNewerPosition, resetOlderPosition) = selectKeyValueAndDecideOffsetResets(maybeNewerKeyValue, maybeOlderKeyValue)
        _                                                  = if (resetNewerPosition) newerFileChannel.position(newerStartPosition)
        _                                                  = if (resetOlderPosition) olderFileChannel.position(olderStartPosition)
      } yield treeMap.updated(keyValue.k, keyValue.v)).flatMap(updatedTree => go(updatedTree))
    }
  }
  go(TreeMap[String, String]())
}

private def getMaybeKeyValue(fileChannel: FileChannel): EitherT[IO, DatabaseException, Option[KeyValuePair]] =
  if (fileChannel.position() == fileChannel.size()) {
    EitherT.pure[IO, DatabaseException](None)
  } else {
    readKeyValueFromFileChannel(fileChannel).map(Some(_))
  }

private def selectKeyValueAndDecideOffsetResets(
  maybeNewerKeyValue: Option[KeyValuePair],
  maybeOlderKeyValue: Option[KeyValuePair]
): (KeyValuePair, Boolean, Boolean) =
  (maybeNewerKeyValue, maybeOlderKeyValue) match {
    case (Some(newerKeyValue), Some(olderKeyValue)) =>
      if (newerKeyValue.k == olderKeyValue.k) {
        (newerKeyValue, false, false)
      } else if (newerKeyValue.k < olderKeyValue.v) {
        (newerKeyValue, false, true)
      } else {
        (olderKeyValue, true, false)
      }
    case (Some(newerKeyValue), None) => (newerKeyValue, false, false)
    case (None, Some(olderKeyValue)) => (olderKeyValue, false, false)
    case (None, None)                => ???
  }

private def tryIO[T](f: T): IO[T] = IO.fromTry(Try(f))
