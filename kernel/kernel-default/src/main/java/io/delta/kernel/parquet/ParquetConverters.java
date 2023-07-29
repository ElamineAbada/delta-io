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
package io.delta.kernel.parquet;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import org.apache.parquet.schema.Type;

import io.delta.kernel.DefaultKernelUtils;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.vector.DefaultBinaryVector;
import io.delta.kernel.data.vector.DefaultBooleanVector;
import io.delta.kernel.data.vector.DefaultByteVector;
import io.delta.kernel.data.vector.DefaultConstantVector;
import io.delta.kernel.data.vector.DefaultDoubleVector;
import io.delta.kernel.data.vector.DefaultFloatVector;
import io.delta.kernel.data.vector.DefaultIntVector;
import io.delta.kernel.data.vector.DefaultLongVector;
import io.delta.kernel.data.vector.DefaultShortVector;
import io.delta.kernel.types.*;

class ParquetConverters
{
    public static Converter createConverter(
        int initialBatchSize,
        DataType typeFromClient,
        Type typeFromFile
    )
    {
        if (typeFromClient instanceof StructType) {
            return new RowConverter(
                initialBatchSize,
                (StructType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof ArrayType) {
            return new ArrayConverter(
                initialBatchSize,
                (ArrayType) typeFromClient,
                (GroupType) typeFromFile
            );
        }
        else if (typeFromClient instanceof MapType) {
            return new MapConverter(
                initialBatchSize,
                (MapType) typeFromClient,
                (GroupType) typeFromFile);
        }
        else if (typeFromClient instanceof StringType || typeFromClient instanceof BinaryType) {
            return new BinaryColumnConverter(typeFromClient, initialBatchSize);
        }
        else if (typeFromClient instanceof BooleanType) {
            return new BooleanColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof IntegerType || typeFromClient instanceof DateType) {
            return new IntColumnConverter(typeFromClient, initialBatchSize);
        }
        else if (typeFromClient instanceof ByteType) {
            return new ByteColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof ShortType) {
            return new ShortColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof LongType) {
            return new LongColumnConverter(typeFromClient, initialBatchSize);
        }
        else if (typeFromClient instanceof FloatType) {
            return new FloatColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof DoubleType) {
            return new DoubleColumnConverter(initialBatchSize);
        }
        else if (typeFromClient instanceof TimestampType) {
            PrimitiveType primType = typeFromFile.asPrimitiveType();

            if (primType.getPrimitiveTypeName() == INT96) {

                return new TimestampBinaryConverter(initialBatchSize);

            } else if (primType.getPrimitiveTypeName() == INT64) {

                LogicalTypeAnnotation typeAnnotation = primType.getLogicalTypeAnnotation();
                if (!(typeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)) {
                    throw new IllegalStateException(String.format(
                        "Invalid parquet type %s for timestamp column",
                        typeFromFile));
                }
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) typeAnnotation;
                checkArgument(timestamp.isAdjustedToUTC(),
                    "TimestampType must have parquet TimeType(isAdjustedToUTC=true)");

                if (timestamp.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
                    return new LongColumnConverter(typeFromClient, initialBatchSize);
                } else if (timestamp.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                    return new TimestampMillisConverter(initialBatchSize);
                } else {
                    throw new UnsupportedOperationException(String.format(
                        "Unsupported parquet TimeType unit=%s", timestamp.getUnit()));
                }

            } else {
                throw new IllegalStateException(String.format(
                    "Invalid parquet type %s for timestamp column",
                    typeFromFile));
            }
        }
//        else if (typeFromClient instanceof DecimalType) {
//
//        }

        throw new UnsupportedOperationException(typeFromClient + " is not supported");
    }

    public interface BaseConverter
    {
        ColumnVector getDataColumnVector(int batchSize);

        /**
         * Move the converter to accept the next row value.
         *
         * @return True if the last converted value is null, false otherwise
         */
        boolean moveToNextRow();

        default void resizeIfNeeded() {}

        default void resetWorkingState() {}
    }

    public static class NonExistentColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        private final DataType dataType;

        public NonExistentColumnConverter(DataType dataType)
        {
            this.dataType = Objects.requireNonNull(dataType, "dataType is null");
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            return new DefaultConstantVector(dataType, batchSize, null);
        }

        @Override
        public boolean moveToNextRow()
        {
            return true;
        }
    }

    public abstract static class BasePrimitiveColumnConverter
        extends PrimitiveConverter
        implements BaseConverter
    {
        // working state
        protected int currentRowIndex;
        protected boolean[] nullability;

        BasePrimitiveColumnConverter(int initialBatchSize)
        {
            checkArgument(initialBatchSize >= 0, "invalid initialBatchSize: %s", initialBatchSize);

            // Initialize the working state
            this.nullability = initNullabilityVector(initialBatchSize);
        }

        @Override
        public boolean moveToNextRow()
        {
            resizeIfNeeded();
            currentRowIndex++;
            return this.nullability[currentRowIndex - 1];
        }
    }

    public static class BooleanColumnConverter
        extends BasePrimitiveColumnConverter
    {
        // working state
        private boolean[] values;

        BooleanColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new boolean[initialBatchSize];
        }

        @Override
        public void addBoolean(boolean value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultBooleanVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new boolean[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ByteColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private byte[] values;

        ByteColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new byte[initialBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (byte) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultByteVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new byte[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class ShortColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private short[] values;

        ShortColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new short[initialBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = (short) value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultShortVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new short[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class IntColumnConverter
        extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;
        // working state
        private int[] values;

        IntColumnConverter(DataType dataType, int initialBatchSize)
        {
            super(initialBatchSize);
            checkArgument(dataType instanceof IntegerType || dataType instanceof DataType);
            this.dataType = dataType;
            this.values = new int[initialBatchSize];
        }

        @Override
        public void addInt(int value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultIntVector(dataType, batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new int[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class LongColumnConverter
        extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;
        // working state
        private long[] values;

        LongColumnConverter(DataType dataType, int initialBatchSize)
        {
            super(initialBatchSize);
            checkArgument(dataType instanceof LongType || dataType instanceof TimestampType);
            this.dataType = dataType;
            this.values = new long[initialBatchSize];
        }

        @Override
        public void addLong(long value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultLongVector(dataType, batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new long[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class TimestampMillisConverter extends LongColumnConverter {

        TimestampMillisConverter( int initialBatchSize) {
            super(TimestampType.INSTANCE, initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            super.addLong(DefaultKernelUtils.millisToMicros(value));
        }
    }

    public static class TimestampBinaryConverter extends LongColumnConverter {

        TimestampBinaryConverter( int initialBatchSize) {
            super(TimestampType.INSTANCE, initialBatchSize);
        }

        private long binaryToSQLTimestamp(Binary binary) {
            checkArgument(binary.length() == 12, String.format(
                "Timestamps (with nanoseconds) are expected to be stored in 12-byte long " +
                    "binaries. Found a %s-byte binary instead.", binary.length()
                ));
            ByteBuffer buffer = binary.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
            long timeOfDayNanos = buffer.getLong();
            int julianDay = buffer.getInt();
            return DefaultKernelUtils.fromJulianDay(julianDay, timeOfDayNanos);
        }

        @Override
        public void addBinary(Binary value) {
            long julianMicros = binaryToSQLTimestamp(value);
            // we do not rebase timestamps
            long gregorianMicros = julianMicros;
            super.addLong(gregorianMicros);
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException(getClass().getName());
        }
    }


    public static class FloatColumnConverter
        extends BasePrimitiveColumnConverter
    {
        // working state
        private float[] values;

        FloatColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new float[initialBatchSize];
        }

        @Override
        public void addFloat(float value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultFloatVector(batchSize, Optional.of(nullability), values);
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new float[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class DoubleColumnConverter
        extends BasePrimitiveColumnConverter
    {

        // working state
        private double[] values;

        DoubleColumnConverter(int initialBatchSize)
        {
            super(initialBatchSize);
            this.values = new double[initialBatchSize];
        }

        @Override
        public void addDouble(double value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value;
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector =
                new DefaultDoubleVector(batchSize, Optional.of(nullability), values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new double[values.length];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class BinaryColumnConverter
        extends BasePrimitiveColumnConverter
    {
        private final DataType dataType;

        // working state
        private byte[][] values;

        BinaryColumnConverter(DataType dataType, int initialBatchSize)
        {
            super(initialBatchSize);
            this.dataType = dataType;
            this.values = new byte[initialBatchSize][];
        }

        @Override
        public void addBinary(Binary value)
        {
            resizeIfNeeded();
            this.nullability[currentRowIndex] = false;
            this.values[currentRowIndex] = value.getBytes();
        }

        @Override
        public ColumnVector getDataColumnVector(int batchSize)
        {
            ColumnVector vector = new DefaultBinaryVector(dataType, batchSize, values);
            // re-initialize the working space
            this.nullability = initNullabilityVector(nullability.length);
            this.values = new byte[values.length][];
            this.currentRowIndex = 0;
            return vector;
        }

        @Override
        public void resizeIfNeeded()
        {
            if (values.length == currentRowIndex) {
                int newSize = values.length * 2;
                this.values = Arrays.copyOf(this.values, newSize);
                this.nullability = Arrays.copyOf(this.nullability, newSize);
                setNullabilityToTrue(this.nullability, newSize / 2, newSize);
            }
        }
    }

    public static class FileRowIndexColumnConverter
            extends LongColumnConverter {

        public FileRowIndexColumnConverter(int initialBatchSize) {
            super(LongType.INSTANCE, initialBatchSize);
        }

        @Override
        public void addLong(long value) {
            throw new UnsupportedOperationException("cannot add long to metadata column");
        }

        /**
         * @param fileRowIndex the file row index of the row processed
         */
        // If moveToNextRow() is called instead the value will be null
        public boolean moveToNextRow(long fileRowIndex) {
            super.values[currentRowIndex] = fileRowIndex;
            this.nullability[currentRowIndex] = false;
            return moveToNextRow();
        }
    }

    static boolean[] initNullabilityVector(int size)
    {
        boolean[] nullability = new boolean[size];
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, true);

        return nullability;
    }

    static void setNullabilityToTrue(boolean[] nullability, int start, int end)
    {
        // Initialize all values as null. As Parquet calls this converter only for non-null
        // values, make the corresponding value to false.
        Arrays.fill(nullability, start, end, true);
    }
}
