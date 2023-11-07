import cats.effect.unsafe.implicits.global
import model.{DatabaseMetadata, LogFile}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import java.nio.file.{Files, Path, Paths}

class DatabaseFileUtilsTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TableDrivenPropertyChecks {
  private val prefix               = "./src/test/resources"
  private val logFile              = "logFile1.txt"
  private val databaseName         = "myDatabase"
  private val existingDatabasePath = Paths.get(s"$prefix/existingDatabase")
  private val databasePath         = Paths.get(s"$prefix/$databaseName")
  private val logFilePath          = Paths.get(s"$prefix/$databaseName/$logFile")
  private val existingLogFilePath  = Paths.get(s"$prefix/existingDatabase/$logFile")
  private val myKey                = "myKey"
  private val keySize              = "00000101"
  private val myValue              = "myValue"
  private val valueSize            = "00000111"

  "create database engine" should "create a new folder and log file" in {
    val expectedDatabase = DatabaseMetadata(databasePath, List(LogFile(logFile, Map())))
    createDatabaseEngine(prefix, "myDatabase").unsafeRunSync() shouldBe expectedDatabase
    Files.isDirectory(databasePath) shouldBe true
    Files.isRegularFile(Paths.get(prefix + "/" + databaseName + "/" + logFile)) shouldBe true
  }

  "writeToFile" should "write to file and return the index it wrote to" in {
    val db            = DatabaseMetadata(existingDatabasePath, List(LogFile(logFile, Map())))
    val stringToWrite = "someStringWithIndices"
    writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync() shouldBe 0
    Files.readString(existingLogFilePath) shouldBe stringToWrite
  }

  it should "write to a file with existing content in and return the index it wrote to" in {
    val db            = DatabaseMetadata(existingDatabasePath, List(LogFile(logFile, Map())))
    val stringToWrite = "someStringWithIndices"

    val i1 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()
    val i2 = writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()

    Files.readString(existingLogFilePath) shouldBe stringToWrite + stringToWrite
    i1 shouldBe 0
    i2 shouldBe stringToWrite.size
  }

  "readFromFile" should "read bytes from a file" in {
    val stringToWrite = keySize + myKey + valueSize + myValue

    writeToFile(stringToWrite, existingLogFilePath).unsafeRunSync()

    readFromFile(0, existingLogFilePath).unsafeRunSync() shouldBe (myKey, myValue)
  }

  it should "read bytes from a file when the offset is greater than 0" in {
    val firstKeyValueString = "someKeyValueString"
    val otherKeyValueString = keySize + myKey + valueSize + myValue

    writeToFile(firstKeyValueString + otherKeyValueString, existingLogFilePath).unsafeRunSync()

    readFromFile(firstKeyValueString.length, existingLogFilePath).unsafeRunSync() shouldBe (myKey, myValue)
  }

  it should "return an error if the keySize or valueSize is not an 8 padded length binary string" in {
    // TODO make the result nicer e.g. either
    val notACorrectlySizedBinaryString = "000"

    val incorrectlyFormattedThings = List(
      notACorrectlySizedBinaryString + myKey + valueSize + myValue,
      keySize + myKey + notACorrectlySizedBinaryString + myValue
    )

    incorrectlyFormattedThings.foreach(s => {
      writeToFile(s, existingLogFilePath).unsafeRunSync()
      assertThrows[NumberFormatException](readFromFile(0, existingLogFilePath).unsafeRunSync())
      Files.writeString(existingLogFilePath, "")
    })
  }

  override def afterEach(): Unit = {
    Files.deleteIfExists(logFilePath)
    Files.deleteIfExists(databasePath)
    Files.writeString(existingLogFilePath, "")
  }
}
