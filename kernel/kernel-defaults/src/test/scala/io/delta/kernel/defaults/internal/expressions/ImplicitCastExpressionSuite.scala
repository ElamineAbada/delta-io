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
package io.delta.kernel.defaults.internal.expressions

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.internal.data.vector.VectorUtils.getValueAsObject
import io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.expressions.Column
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

class ImplicitCastExpressionSuite extends AnyFunSuite with TestUtils {
  private val allowedCasts: Set[(DataType, DataType)] = Set(
    (ByteType.INSTANCE, ShortType.INSTANCE),
    (ByteType.INSTANCE, IntegerType.INSTANCE),
    (ByteType.INSTANCE, LongType.INSTANCE),
    (ByteType.INSTANCE, FloatType.INSTANCE),
    (ByteType.INSTANCE, DoubleType.INSTANCE),

    (ShortType.INSTANCE, IntegerType.INSTANCE),
    (ShortType.INSTANCE, LongType.INSTANCE),
    (ShortType.INSTANCE, FloatType.INSTANCE),
    (ShortType.INSTANCE, DoubleType.INSTANCE),

    (IntegerType.INSTANCE, LongType.INSTANCE),
    (IntegerType.INSTANCE, FloatType.INSTANCE),
    (IntegerType.INSTANCE, DoubleType.INSTANCE),

    (LongType.INSTANCE, FloatType.INSTANCE),
    (LongType.INSTANCE, DoubleType.INSTANCE),
    (FloatType.INSTANCE, DoubleType.INSTANCE))

  test("can cast to") {
    Seq.range(0, ALL_TYPES.length).foreach { fromTypeIdx =>
      val fromType: DataType = ALL_TYPES(fromTypeIdx)
      Seq.range(0, ALL_TYPES.length).foreach { toTypeIdx =>
        val toType: DataType = ALL_TYPES(toTypeIdx)
        assert(canCastTo(fromType, toType) ===
          allowedCasts.contains((fromType, toType)))
      }
    }
  }

  allowedCasts.foreach { castPair =>
    test(s"eval cast expression: ${castPair._1} -> ${castPair._2}") {
      val fromType = castPair._1
      val toType = castPair._2
      val inputVector = testData(87, fromType, (rowId) => rowId % 7 == 0)
      val outputVector = new ImplicitCastExpression(new Column("id"), toType)
        .eval(inputVector)
      checkCastOutput(inputVector, toType, outputVector)
    }
  }

  def testData(size: Int, dataType: DataType, nullability: (Int) => Boolean): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = dataType
      override def getSize: Int = size
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = nullability(rowId)

      override def getByte(rowId: Int): Byte = {
        assert(dataType === ByteType.INSTANCE)
        generateValue(rowId).toByte
      }

      override def getShort(rowId: Int): Short = {
        assert(dataType === ShortType.INSTANCE)
        generateValue(rowId).toShort
      }

      override def getInt(rowId: Int): Int = {
        assert(dataType === IntegerType.INSTANCE)
        generateValue(rowId).toInt
      }

      override def getLong(rowId: Int): Long = {
        assert(dataType === LongType.INSTANCE)
        generateValue(rowId).toLong
      }

      override def getFloat(rowId: Int): Float = {
        assert(dataType === FloatType.INSTANCE)
        generateValue(rowId).toFloat
      }

      override def getDouble(rowId: Int): Double = {
        assert(dataType === DoubleType.INSTANCE)
        generateValue(rowId)
      }
    }
  }

  // Utility method to generate a value based on the rowId. Returned value is a double
  // which the callers can cast to appropriate numerical type.
  private def generateValue(rowId: Int): Double = rowId * 2.76 + 7623

  private def checkCastOutput(input: ColumnVector, toType: DataType, output: ColumnVector): Unit = {
    assert(input.getSize === output.getSize)
    assert(toType === output.getDataType)
    Seq.range(0, input.getSize).foreach { rowId =>
      assert(input.isNullAt(rowId) === output.isNullAt(rowId))
      assert(getValueAsObject(input, rowId) === getValueAsObject(output, rowId))
    }
  }
}
