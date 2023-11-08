import cats.data.EitherT
import cats.effect.unsafe.implicits.global
import model.{DatabaseMetadata, LogFile}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class DatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val logFileName          = "logFile2.txt"
  private val existingDatabasePath = Paths.get("./src/test/resources/existingDatabase")
  private val existingLogFilePath  = Paths.get(s"./src/test/resources/existingDatabase/$logFileName")
  private val myKey                = "myKey"
  private val keySize              = "00000101"
  private val myValue              = "myValue"
  private val valueSize            = "00000111"

  "write" should "update the index when it is empty" in {
    val databaseEngine = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map())))
    storeKeyValue(myKey, myValue, databaseEngine).unsafeRunSync() shouldBe
      Right(databaseEngine.withUpdatedLogFileIndex(Map(myKey -> 0)))
    Files.readString(existingLogFilePath) shouldBe keySize + myKey + valueSize + myValue
  }

  it should "update the index when it is not empty" in {
    val existingData = "someKeyAndValue"
    Files.writeString(existingLogFilePath, existingData)
    val indexMap         = Map("otherKey" -> 0.toLong)
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, indexMap)))
    storeKeyValue(myKey, myValue, databaseMetadata).unsafeRunSync() shouldBe
      Right(databaseMetadata.withUpdatedLogFileIndex(indexMap.updated(myKey, existingData.length)))
    Files.readString(existingLogFilePath) shouldBe existingData + keySize + myKey + valueSize + myValue
  }

  it should "store a key value and retrieve it" in {
    val databaseMetadata = DatabaseMetadata(existingDatabasePath, List(LogFile(existingLogFilePath, Map())))
    val secondKey        = "anotherKey"
    val secondValue      = "anotherValue"
    val result = (for {
      updatedMetadata1     <- EitherT.apply(storeKeyValue(myKey, myValue, databaseMetadata))
      updatedMetadata2     <- EitherT.apply(storeKeyValue(secondKey, secondValue, updatedMetadata1))
      firstRetrievedValue  <- EitherT.right(getFromKey(myKey, updatedMetadata2))
      secondRetrievedValue <- EitherT.right(getFromKey(secondKey, updatedMetadata2))
    } yield (firstRetrievedValue, secondRetrievedValue)).value.unsafeRunSync()
    val (firstRetrievedValue, secondRetrievedValue) = result.getOrElse(throw new RuntimeException("Didn't get a right"))

    result shouldBe Right((myValue, secondValue))
  }

  override def afterEach() = Files.writeString(existingLogFilePath, "")
}
