package SSTKeyValueStore

import model.{DatabaseException, KeyNotFoundInIndices}

import scala.collection.immutable.TreeMap

def write(metadata: SSTDatabaseMetadata, key: String, value: String): TreeMap[String, String] =
  metadata.memTable.updated(key, value)

def read(metadata: SSTDatabaseMetadata, key: String): Either[DatabaseException, String] =
  metadata.memTable.get(key) match {
    case Some(value) => Right(value)
    case None        => Left(KeyNotFoundInIndices(key))
  }
