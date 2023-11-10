package SSTKeyValueStore

import model.KeyNotFoundInIndices
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.TreeMap

class SSTDatabaseEngineIntegrationTest extends AnyFlatSpec with Matchers {
  private val myKey   = "myKey"
  private val myValue = "myValue"
  "write" should "write to a thing" in {
    write(SSTDatabaseMetadata(TreeMap(), List()), myKey, myValue) shouldBe TreeMap(myKey -> myValue)
  }

  "read" should "read from the map" in {
    val memTable = TreeMap(myKey -> myValue)
    read(SSTDatabaseMetadata(memTable, List()), myKey) shouldBe Right(myValue)
  }

  it should "return a left when the value is not found" in {
    read(SSTDatabaseMetadata(TreeMap(), List()), myKey) shouldBe Left(KeyNotFoundInIndices(myKey))
  }
}
