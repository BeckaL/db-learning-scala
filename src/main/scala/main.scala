import cats.effect.*
import model.*
import shared.{createSSTDatabaseEngine, createSimpleDatabaseEngine}

import scala.io.StdIn.readLine
import cats.effect.unsafe.implicits.global //YOLO this shouldn't be here but ah well

def main(args: Array[String]): Unit = {
  var metadata = getDatabaseTypeAndMetadata()
  println("Input your commands. Press h for help or q to quit")
  var action = getNextAction()
  while (action != Quit) {
    metadata = maybeCompressLogFiles(metadata)
    metadata = doAction(action, metadata)
    action = getNextAction()
  }
  println("Goodbye")
}

private def maybeCompressLogFiles(metadata: DatabaseMetadata): DatabaseMetadata = {
  if (metadata.logFiles.size > 2) {
    println("compressing")
    val blah: IO[Either[DatabaseException, DatabaseMetadata]] = metadata match {
      case simpleMetadata: SimpleDatabaseMetadata => SimpleKeyValueStore.compress(simpleMetadata)
      case sstMetadata: SSTDatabaseMetadata       => SSTKeyValueStore.compress(sstMetadata)
    }
    blah.unsafeRunSync() match {
      case Right(newMetadata) => newMetadata
      case Left(error) =>
        println(error.message)
        metadata
    }
  } else metadata
}

private def getDatabaseTypeAndMetadata(): DatabaseMetadata = {
  var databaseType = readLine("What kind of database would you like? Type 1 for simple key value store and 2 for SST key value store")
  while (!Set("1", "2").contains(databaseType)) {
    databaseType = readLine("Didn't understand that, type 1 or 2")
  }
  val name = readLine("What would you like your database to be called?")
  if (databaseType == "1") {
    createSimpleDatabaseEngine(name = name, 200L).unsafeRunSync()
  } else {
    createSSTDatabaseEngine(name = name).unsafeRunSync()
  }
}

private def getNextAction(): Action =
  val input = readLine()
  input.split(" ").toList match {
    case "q" :: nil                               => Quit
    case "s" :: keyToStore :: valueToStore :: nil => Store(keyToStore, valueToStore)
    case "r" :: keyToRead :: nil                  => Read(keyToRead)
    case "h" :: nil                               => Help
    case _                                        => Unknown
  }

private def doAction(action: Action, initialMd: DatabaseMetadata): DatabaseMetadata = {
  var dbToReturn = initialMd
  val helpString = "To store a key value, use command `s [myKey] [myValue]`. \nTo read, use `r [myKey]`. \nTo quit, press q\n"
  action match {
    case Store(keyToStore, valueToStore) => dbToReturn = store(keyToStore, valueToStore, initialMd)
    case Read(keyToRead)                 => println(getKey(keyToRead, initialMd))
    case Unknown                         => println(s"Didn't understand instruction \n$helpString")
    case Help                            => println(helpString)
  }
  dbToReturn
}

private def store(
  keyToStore: String,
  valueToStore: String,
  metadata: DatabaseMetadata
): DatabaseMetadata =
  val io: IO[Either[DatabaseException, DatabaseMetadata]] = metadata match {
    case simpleMetadata: SimpleDatabaseMetadata => SimpleKeyValueStore.storeKeyValue(keyToStore, valueToStore, simpleMetadata)
    case sstMetadata: SSTDatabaseMetadata =>
      SSTKeyValueStore.write(sstMetadata, keyToStore, valueToStore).map((updatedMetadata: SSTDatabaseMetadata) => Right(updatedMetadata))
  }
  io.unsafeRunSync() match {
    case Right(updatedMetadata) =>
      println("Successfully stored")
      updatedMetadata
    case Left(err) =>
      println(err.message)
      metadata
  }

private def getKey(keyToRead: String, md: DatabaseMetadata): String =
  val blah = md match {
    case simpleMetadata: SimpleDatabaseMetadata => SimpleKeyValueStore.getFromKey(keyToRead, simpleMetadata)
    case sstMetadata: SSTDatabaseMetadata       => SSTKeyValueStore.read(sstMetadata, keyToRead)
  }
  blah.unsafeRunSync() match {
    case Right(s) => s
    case Left(e)  => e.message
  }
