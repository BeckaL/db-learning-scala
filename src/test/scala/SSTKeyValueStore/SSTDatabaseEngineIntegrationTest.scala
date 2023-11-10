package SSTKeyValueStore

import cats.effect.unsafe.implicits.global
import model.{KeyNotFoundInIndices, LogFile}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import shared.getStringToWrite

import java.nio.file.{Files, Paths}
import scala.collection.immutable.TreeMap

class SSTDatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach{
  private val myKey   = "myKey"
  private val myValue = "myValue"
  private val databasePath = Paths.get("./src/test/resources/SSTDatabaseEngineIntegrationTest")
  private val existingLogFile = Paths.get(databasePath.toString + "/" + "logFile1.txt")
  "write" should "write to a thing" in {
    write(SSTDatabaseMetadata(TreeMap(), List()), myKey, myValue) shouldBe TreeMap(myKey -> myValue)
  }

  "read" should "read from the map" in {
    val memTable = TreeMap(myKey -> myValue)
    read(SSTDatabaseMetadata(memTable, List()), myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "read from a logfile when the entry is directly in the index" in {
    Files.writeString(existingLogFile, getStringToWrite(myKey, myValue).getOrElse(throw new RuntimeException("oops")))
    val logFile = LogFile(existingLogFile, Map(myKey -> 0))
    val metadata = SSTDatabaseMetadata(TreeMap("anotherKey" -> "anotherValue"), List(logFile))

    read(metadata, myKey).unsafeRunSync() shouldBe Right(myValue)
  }

  it should "return a left when the value is not found in the memtable and there are no logs" in {
    read(SSTDatabaseMetadata(TreeMap(), List()), myKey).unsafeRunSync() shouldBe Left(KeyNotFoundInIndices(myKey))
  }

  override def afterEach(): Unit =
    Files.writeString(existingLogFile, "")
}
