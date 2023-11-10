package SSTKeyValueStore

import model.{DatabaseException, KeyNotFoundInIndices}

import scala.collection.immutable.TreeMap

def write(treeMap: TreeMap[String, String], key: String, value: String): TreeMap[String, String] =
  treeMap.updated(key, value)

def read(treeMap: TreeMap[String, String], key: String): Either[DatabaseException, String] =
  treeMap.get(key) match {
    case Some(value) => Right(value)
    case None        => Left(KeyNotFoundInIndices(key))
  }
