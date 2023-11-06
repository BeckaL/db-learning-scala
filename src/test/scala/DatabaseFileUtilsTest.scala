import cats.effect.unsafe.implicits.global
import model.{DatabaseMetadata, LogFile}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

class DatabaseFileUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  val prefix       = "./src/test/resources"
  val logFile      = "logFile1.txt"
  val databaseName = "myDatabase"
  val existingDatabasePath = Paths.get(s"$prefix/existingDatabase")
  val databasePath  = Paths.get(s"$prefix/$databaseName")
  val logFilePath = Paths.get(s"$prefix/$databaseName/$logFile")
  val existingLogFilePath = Paths.get(s"$prefix/existingDatabase/$logFile")


  "create database engine" should "create a new folder and log file" in {
    val expectedDatabase = DatabaseMetadata(databasePath, List(LogFile(logFile, Map())))
    createDatabaseEngine(prefix, "myDatabase").unsafeRunSync() shouldBe expectedDatabase
    Files.isDirectory(databasePath) shouldBe true
    Files.isRegularFile(Paths.get(prefix + "/" + databaseName + "/" + logFile)) shouldBe true
  }

  "writeToFile" should "write to file and return the index it wrote to" in {
    val db = DatabaseMetadata(existingDatabasePath, List(LogFile(logFile, Map())))
    val stringToWrite = "someStringWithIndices"
    writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync() shouldBe 0
    Files.readString(existingLogFilePath) shouldBe stringToWrite
  }

  it should "write to a file with existing content in and return the index it wrote to" in {
    val db = DatabaseMetadata(existingDatabasePath, List(LogFile(logFile, Map())))
    val stringToWrite = "someStringWithIndices"

    val i1 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()
    val i2 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()

    Files.readString(existingLogFilePath) shouldBe stringToWrite + stringToWrite
    i1 shouldBe 0
    i2 shouldBe stringToWrite.size
  }

  override def afterEach(): Unit = {
    Files.deleteIfExists(logFilePath)
    Files.deleteIfExists(databasePath)
    Files.writeString(existingLogFilePath, "")
  }
}
