import cats.data.EitherT
import cats.effect.IO
import model.DatabaseMetadata

import java.nio.file.Path

def storeKeyValue(key: String, value: String, engine: DatabaseMetadata): IO[Either[String, DatabaseMetadata]] =
  (for {
    string <- EitherT.fromEither[IO](getStringToWrite(key, value))
    i      <- EitherT.right(writeToFile(string, engine.indices.head.path))
  } yield engine.withUpdatedLogFileIndex(engine.indices.head.index.updated(key, i))).value

def getFromKey(key: String, metadata: DatabaseMetadata): IO[String] = {
  val logFile = metadata.indices.head
  val offset  = metadata.indices.head.index(key)
  // TODO error handling if key does not match
  readFromFile(offset, logFile.path).map(result => result._2)
}
private def getStringToWrite(key: String, value: String): Either[String, String] =
  for {
    keySize   <- toPaddedBinaryString(key.size)
    valueSize <- toPaddedBinaryString(value.size)
  } yield keySize + key + valueSize + value

private def toPaddedBinaryString(i: Int): Either[String, String] =
  val binString = i.toBinaryString
  if (binString.length > 8) {
    Left(s"Tried to store string of length $i, this was more than the allowed size")
  } else {
    Right("0" * (8 - binString.length) + binString)
  }


