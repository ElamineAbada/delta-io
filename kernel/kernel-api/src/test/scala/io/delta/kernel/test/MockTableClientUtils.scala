/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.test

import io.delta.kernel.client._
import io.delta.kernel.data.{ColumnVector, ColumnarBatch}
import io.delta.kernel.expressions.{Expression, ExpressionEvaluator, Predicate, PredicateEvaluator}
import io.delta.kernel.types.{DataType, StructType}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import java.io.ByteArrayInputStream
import java.util.Optional

/**
 * Contains broiler plate code for mocking [[TableClient]] and its sub-interfaces.
 *
 * A concrete class is created for each sub-interface (e.g. [[FileSystemClient]]) with
 * default implementation (unsupported). Test suites can override a specific API(s)
 * in the sub-interfaces to mock the behavior as desired.
 *
 * Example:
 * {{{
 *   val myMockFileSystemClient = new BaseMockFileSystemClient() {
 *     override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
 *        .. my mock code to return specific values for given file path ...
 *     }
 *   }
 *
 *   val myMockTableClient = mockTableClient(fileSystemClient = myMockFileSystemClient)
 * }}}
 */
trait MockTableClientUtils {
  /**
   * Create a mock TableClient with the given components. If a component is not provided, it will
   * throw an exception when accessed.
   */
  def mockTableClient(
    fileSystemClient: FileSystemClient = null,
    jsonHandler: JsonHandler = null,
    parquetHandler: ParquetHandler = null,
    expressionHandler: ExpressionHandler = null): TableClient = {
    new TableClient() {
      override def getExpressionHandler: ExpressionHandler =
        Option(expressionHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getJsonHandler: JsonHandler =
        Option(jsonHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getFileSystemClient: FileSystemClient =
        Option(fileSystemClient).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getParquetHandler: ParquetHandler =
        Option(parquetHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))
    }
  }
}

/**
 * Base class for mocking [[JsonHandler]]
 */
trait BaseMockJsonHandler extends JsonHandler {
  override def parseJson(
      jsonStringVector: ColumnVector,
      outputSchema: StructType,
      selectionVector: Optional[ColumnVector]): ColumnarBatch =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def deserializeStructType(structTypeJson: String): StructType =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for mocking [[ParquetHandler]]
 */
trait BaseMockParquetHandler extends ParquetHandler {
  override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for mocking [[ExpressionHandler]]
 */
trait BaseMockExpressionHandler extends ExpressionHandler {
  override def getPredicateEvaluator(
      inputSchema: StructType,
      predicate: Predicate): PredicateEvaluator =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def getEvaluator(
      inputSchema: StructType,
      expression: Expression,
      outputType: DataType): ExpressionEvaluator =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def isSupported(
      inputSchema: StructType,
      expression: Expression,
      outputType: DataType): Boolean =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def createSelectionVector(values: Array[Boolean], from: Int, to: Int): ColumnVector =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for [[FileSystemClient]]
 */
trait BaseMockFileSystemClient extends FileSystemClient {
  override def listFrom(filePath: String): CloseableIterator[FileStatus] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def resolvePath(path: String): String =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def readFiles(
      readRequests: CloseableIterator[FileReadRequest]): CloseableIterator[ByteArrayInputStream] =
    throw new UnsupportedOperationException("not supported in this test suite")
}

