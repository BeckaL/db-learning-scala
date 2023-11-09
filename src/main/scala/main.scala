import cats.effect.*
import model.{DatabaseException, DatabaseMetadata}

import scala.io.StdIn.readLine
import cats.effect.unsafe.implicits.global //YOLO this shouldn't be here but ah well

def main(args: Array[String]): Unit = {
  val name     = readLine("What would you like your database to be called?")
  var metadata = createDatabaseEngine(name = name, 200L).unsafeRunSync()
  val inputString =
    "Input your commands. \nTo store a key value, use command `s [myKey] [myValue]`. \nTo read, use `r [myKey]`. \nTo quit, press q\n"
  var input = readLine(inputString)
  while (input.trim != "q") {
    if (metadata.logFiles.size > 2) {
      println("compressing")
      compress(metadata).unsafeRunSync() match {
        case Left(e) => println(e)
        case Right(compressedMetadata) => metadata = compressedMetadata
      }
    }
    val updatedMetadata = doAction(input.split(" ").toList, metadata)
    metadata = updatedMetadata
    input = readLine(inputString)
  }
}

private def doAction(input: List[String], initialMd: DatabaseMetadata): DatabaseMetadata = {
  var dbToReturn = initialMd
  input match {
    case "s" :: keyToStore :: valueToStore :: nil =>
      store(keyToStore, valueToStore, initialMd) match {
        case Right(s, md) =>
          println(s)
          dbToReturn = md
        case Left(e) => println(e)
      }
    case "r" :: keyToRead :: nil => println(getKey(keyToRead, initialMd))
    case other                   => println(s"Didn't understand instruction ${other}")
  }
  dbToReturn
}

private def store(keyToStore: String, valueToStore: String, metadata: DatabaseMetadata): Either[String, (String, DatabaseMetadata)] =
  storeKeyValue(keyToStore, valueToStore, metadata).unsafeRunSync() match {
    case Right(md) => Right("Successfully stored", md)
    case Left(err) => Left(err.message)
  }

private def getKey(keyToRead: String, md: DatabaseMetadata): String =
  getFromKey(keyToRead, md).unsafeRunSync() match {
    case Right(s) => s
    case Left(e)  => e.message
  }
