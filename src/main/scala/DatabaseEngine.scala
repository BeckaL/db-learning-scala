import cats.data.EitherT
import cats.effect.IO
import model.DatabaseMetadata

import java.nio.file.Path

def storeKeyValue(key: String, value: String, engine: DatabaseMetadata): IO[Either[String, DatabaseMetadata]] =
  (for {
    string <- EitherT.fromEither[IO](getStringToWrite(key, value))
    i <- EitherT.right(writeToFile(string, engine.indices.head.path))
  } yield engine.withUpdatedLogFileIndex(engine.indices.head.index.updated(key, i))).value

private def getStringToWrite(key: String, value: String): Either[String, String] =
  for {
    keySize <- toPaddedBinaryString(key.size)
    valueSize <- toPaddedBinaryString(value.size)
  } yield keySize + key + valueSize + value

private def toPaddedBinaryString(i: Int): Either[String, String] =
  val binString = i.toBinaryString
  if (binString.length > 8) {
    Left(s"Tried to store string of length $i, this was more than the allowed size")
  } else {
    Right("0" * (8 - binString.length) + binString)
  }

implicit class DatabaseMetadataOps(md: DatabaseMetadata) {
  def withUpdatedLogFileIndex(indexMap: Map[String, Long]): DatabaseMetadata = {
    md.copy(indices = md.indices.updated(0, md.indices.head.copy(index = indexMap)))
  }
}