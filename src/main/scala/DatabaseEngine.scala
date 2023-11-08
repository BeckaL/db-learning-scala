import cats.data.EitherT
import cats.effect.IO
import cats.implicits.*
import model.{DatabaseMetadata, LogFile}

import java.nio.file.Path

def storeKeyValue(key: String, value: String, engine: DatabaseMetadata): IO[Either[String, DatabaseMetadata]] =
  (for {
    string <- EitherT.fromEither[IO](getStringToWrite(key, value))
    i      <- EitherT.right(writeToFile(string, engine.indices.head.path))
  } yield engine.withUpdatedLogFileIndex(engine.indices.head.index.updated(key, i))).value

def getFromKey(key: String, metadata: DatabaseMetadata): IO[Either[String, String]] = 
  findIndexFromLogFiles(key, metadata.indices) match {
    case Left(error) => IO.pure(Left(error))
    // TODO error handling if key does not match
    case Right((path, offset)) => readFromFile(offset, path).map(r => Right(r._2))
  }

private def findIndexFromLogFiles(key: String, logFiles: List[LogFile]): Either[String, (Path, Long)] = 
  logFiles match {
    case Nil => Left(s"Could not find value for key $key")
    case firstLogFile :: others => firstLogFile.index.get(key) match {
        case Some(i) => Right((firstLogFile.path, i))
        case None    => findIndexFromLogFiles(key, others)
      }
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
