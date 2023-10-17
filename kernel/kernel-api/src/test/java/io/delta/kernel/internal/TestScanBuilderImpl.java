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
package test.java.io.delta.kernel.internal;

import org.junit.Test;
import static org.junit.Assert.*;
import io.delta.kernel.internal.ScanBuilderImpl;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.client.TableClient;

public class TestScanBuilderImpl {

    @Test
    public void withFilter_ShouldSetPredicate() {
        // Create a sample ScanBuilderImpl instance
        StructType snapshotSchema = new StructType();
        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        // Create a sample Predicate
        Predicate predicate = new Predicate();

        // Call withFilter
        ScanBuilderImpl resultBuilder = scanBuilder.withFilter(tableClient, predicate);

        // Assert that the resultBuilder is not the same as the original scanBuilder
        assertNotSame(scanBuilder, resultBuilder);

        // Assert that the predicate has been set in the resultBuilder
        assertEquals(predicate, resultBuilder.getPredicate().get());
    }

    @Test
    public void withReadSchema_ShouldSetValidSchema() {
        // Create a sample ScanBuilderImpl instance
        StructType snapshotSchema = new StructType();
        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        // Create a sample StructType for readSchema
        StructType readSchema = new StructType();

        // Call withReadSchema
        ScanBuilderImpl resultBuilder = scanBuilder.withReadSchema(tableClient, readSchema);

        // Assert that the resultBuilder is not the same as the original scanBuilder
        assertNotSame(scanBuilder, resultBuilder);

        // Assert that the readSchema has been set in the resultBuilder
        assertEquals(readSchema, resultBuilder.getReadSchema());
    }

    @Test
    public void withReadSchema_OverlappingFields_ShouldThrowException() {
        // Create a sample ScanBuilderImpl instance with a snapshot schema
        StructType snapshotSchema = new StructType()
                .add("field1", StructType.from(DataType.IntegerType), true);

        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        // Try to set a readSchema with overlapping fields but different data types
        StructType readSchema = new StructType()
                .add("field1", StructType.from(DataType.StringType), true);

        // Use assertThrows to verify that it throws an exception
        assertThrows(IllegalArgumentException.class, () -> {
            scanBuilder.withReadSchema(tableClient, readSchema);
        });
    }

    @Test
    public void withReadSchema_SettingEmptyReadSchema_ShouldKeepSnapshotSchema() {
        // Create a sample ScanBuilderImpl instance with a snapshot schema
        StructType snapshotSchema = new StructType()
                .add("field1", StructType.from(DataType.IntegerType), true);

        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        // Create an empty readSchema
        StructType emptyReadSchema = new StructType();

        // Call withReadSchema with an empty readSchema
        ScanBuilderImpl resultBuilder = scanBuilder.withReadSchema(tableClient, emptyReadSchema);

        // Assert that the resultBuilder is not the same as the original scanBuilder
        assertNotSame(scanBuilder, resultBuilder);

        // Assert that the readSchema in the resultBuilder remains the same as the
        // snapshot schema
        assertEquals(snapshotSchema, resultBuilder.getReadSchema());
    }
}
