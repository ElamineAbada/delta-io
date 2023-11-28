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
        StructType snapshotSchema = new StructType();
        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        Predicate predicate = new Predicate();

        ScanBuilderImpl resultBuilder = scanBuilder.withFilter(tableClient, predicate);

        assertNotSame(scanBuilder, resultBuilder);

        assertEquals(predicate, resultBuilder.getPredicate().get());
    }

    @Test
    public void withReadSchema_ShouldSetValidSchema() {
        StructType snapshotSchema = new StructType();
        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        StructType readSchema = new StructType();

        ScanBuilderImpl resultBuilder = scanBuilder.withReadSchema(tableClient, readSchema);

        assertNotSame(scanBuilder, resultBuilder);

        assertEquals(readSchema, resultBuilder.getReadSchema());
    }

    @Test
    public void withReadSchema_OverlappingFields_ShouldThrowException() {
        StructType snapshotSchema = new StructType()
                .add("field1", StructType.from(DataType.IntegerType), true);

        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        StructType readSchema = new StructType()
                .add("field1", StructType.from(DataType.StringType), true);

        assertThrows(IllegalArgumentException.class, () -> {
            scanBuilder.withReadSchema(tableClient, readSchema);
        });
    }

    @Test
    public void withReadSchema_SettingEmptyReadSchema_ShouldKeepSnapshotSchema() {
        StructType snapshotSchema = new StructType()
                .add("field1", StructType.from(DataType.IntegerType), true);

        TableClient tableClient = new MockTableClient();
        ScanBuilderImpl scanBuilder = new ScanBuilderImpl(null, null, snapshotSchema, null, tableClient);

        StructType emptyReadSchema = new StructType();

        ScanBuilderImpl resultBuilder = scanBuilder.withReadSchema(tableClient, emptyReadSchema);

        assertNotSame(scanBuilder, resultBuilder);

        assertEquals(snapshotSchema, resultBuilder.getReadSchema());
    }
}
