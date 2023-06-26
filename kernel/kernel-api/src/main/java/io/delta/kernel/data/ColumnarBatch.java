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

package io.delta.kernel.data;

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import io.delta.kernel.internal.data.ColumnarBatchRow;

/**
 * Represents zero or more rows of records with same schema type.
 */
public interface ColumnarBatch
{
    /**
     * @return the schema of the data in this batch.
     */
    StructType getSchema();

    /**
     * Return the {@link ColumnVector} for the given ordinal in the columnar batch. If the ordinal
     * is not valid throws error.
     *
     * @param ordinal the ordinal of the column to retrieve
     * @return the {@link ColumnVector} for the given ordinal in the columnar batch
     */
    ColumnVector getColumnVector(int ordinal);

    /**
     * @return the number of rows/records in the columnar batch
     */
    int getSize();

    /**
     * Insert the given {@code columnVector} at given {@code ordinal}. Shift the existing
     * {@link ColumnVector}s located at from {@code ordinal} to the end by one position.
     * The schema of the {@link ColumnarBatch} will also be changed to reflect the newly inserted
     * vector.
     *
     * @param ordinal
     * @param columnSchema Column name and schema details of the new column vector.
     * @param columnVector
     * @return
     * @throws IllegalArgumentException If the ordinal is not valid (ie less than zero or
     * greater than the current number of vectors).
     */
    default void insertVector(int ordinal, StructField columnSchema, ColumnVector columnVector)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Update the schema of this {@link ColumnarBatch}. The data types of elements in
     * the given new schema and existing schema should be the same. Rest of the details such as
     * name of the column or column metadata could be different.
     * @param newSchema
     */
    default void updateSchema(StructType newSchema) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Return a slice of the current batch.
     *
     * @param start Starting record index to include in the returned columnar batch
     * @param end Ending record index (exclusive) to include in the returned columnar batch
     * @return a columnar batch containing the records between [start, end)
     */
    default ColumnarBatch slice(int start, int end)
    {
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    /**
     * @return iterator of {@link Row}s in this batch
     */
    default CloseableIterator<Row> getRows()
    {
        final ColumnarBatch batch = this;
        return new CloseableIterator<Row>()
        {
            int rowId = 0;
            int maxRowId = getSize();

            @Override
            public boolean hasNext()
            {
                return rowId < maxRowId;
            }

            @Override
            public Row next()
            {
                Row row = new ColumnarBatchRow(batch, rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() {}
        };
    }
}
