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
package io.delta.kernel.internal.actions;

import java.util.*;
import java.util.stream.Collectors;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.InternalUtils.requireNonNull;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class Metadata {

    public static Metadata fromColumnVector(
            ColumnVector vector, int rowId, Engine engine) {
        if (vector.isNullAt(rowId)) {
            return null;
        }

        final String schemaJson = requireNonNull(vector.getChild(4), rowId, "schemaString")
            .getString(rowId);
        StructType  schema = engine.getJsonHandler().deserializeStructType(schemaJson);

        return new Metadata(
            requireNonNull(vector.getChild(0), rowId, "id").getString(rowId),
            Optional.ofNullable(vector.getChild(1).isNullAt(rowId) ? null :
                vector.getChild(1).getString(rowId)),
            Optional.ofNullable(vector.getChild(2).isNullAt(rowId) ? null :
                vector.getChild(2).getString(rowId)),
            Format.fromColumnVector(requireNonNull(vector.getChild(3), rowId, "format"), rowId),
            schemaJson,
            schema,
            vector.getChild(5).getArray(rowId),
            Optional.ofNullable(vector.getChild(6).isNullAt(rowId) ? null :
                vector.getChild(6).getLong(rowId)),
            VectorUtils.toJavaMap(vector.getChild(7).getMap(rowId))
        );
    }

    public static final StructType FULL_SCHEMA = new StructType()
        .add("id", StringType.STRING, false /* nullable */)
        .add("name", StringType.STRING, true /* nullable */)
        .add("description", StringType.STRING, true /* nullable */)
        .add("format", Format.FULL_SCHEMA, false /* nullable */)
        .add("schemaString", StringType.STRING, false /* nullable */)
        .add("partitionColumns",
            new ArrayType(StringType.STRING, false /* contains null */),
            false /* nullable */)
        .add("createdTime", LongType.LONG, true /* contains null */)
        .add("configuration",
            new MapType(StringType.STRING, StringType.STRING, false),
            false /* nullable */);

    private final String id;
    private final Optional<String> name;
    private final Optional<String> description;
    private final Format format;
    private String schemaString;
    private StructType schema;
    private final ArrayValue partitionColumns;
    private final Optional<Long> createdTime;
    private final Map<String, String> configuration;
    // Partition column names in lower case.
    private final Lazy<Set<String>> partitionColNames;
    // Logical data schema excluding partition columns
    private final Lazy<StructType> dataSchema;

    public Metadata(
        String id,
        Optional<String> name,
        Optional<String> description,
        Format format,
        String schemaString,
        StructType schema,
        ArrayValue partitionColumns,
        Optional<Long> createdTime,
        Map<String, String> configuration) {
        this.id = requireNonNull(id, "id is null");
        this.name = name;
        this.description = requireNonNull(description, "description is null");
        this.format = requireNonNull(format, "format is null");
        this.schemaString = requireNonNull(schemaString, "schemaString is null");
        this.schema = schema;
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.createdTime = createdTime;
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.partitionColNames = new Lazy<>(() -> loadPartitionColNames());
        this.dataSchema = new Lazy<>(() ->
            new StructType(schema.fields().stream()
                .filter(field ->
                    !partitionColNames.get().contains(field.getName().toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList())));
    }

    public String getSchemaString() {
        return schemaString;
    }

    public StructType getSchema() {
        return schema;
    }

    public void updateSchemaString(String schemaString) {
        this.schemaString = schemaString;
    }

    public void updateSchema(StructType schema) {
        this.schema = schema;
    }

    public ArrayValue getPartitionColumns() {
        return partitionColumns;
    }

    /** Set of lowercase partition column names */
    public Set<String> getPartitionColNames() {
        return partitionColNames.get();
    }

    /** The logical data schema which excludes partition columns */
    public StructType getDataSchema() {
        return dataSchema.get();
    }

    public String getId() {
        return id;
    }

    public Optional<String> getName() {
        return name;
    }

    public Optional<String> getDescription() {
        return description;
    }

    public Format getFormat() {
        return format;
    }

    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    public MapValue getConfigurationMapValue() {
        return VectorUtils.stringStringMapValue(configuration);
    }

    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    public void updateConfiguration(Map<String, String> configuration) {
        this.configuration.putAll(configuration);
    }

    /**
     * Encode as a {@link Row} object with the schema {@link Metadata#FULL_SCHEMA}.
     *
     * @return {@link Row} object with the schema {@link Metadata#FULL_SCHEMA}
     */
    public Row toRow() {
        Map<Integer, Object> metadataMap = new HashMap<>();
        metadataMap.put(0, id);
        metadataMap.put(1, name.orElse(null));
        metadataMap.put(2, description.orElse(null));
        metadataMap.put(3, format.toRow());
        metadataMap.put(4, schemaString);
        metadataMap.put(5, partitionColumns);
        metadataMap.put(6, createdTime.orElse(null));
        metadataMap.put(7, getConfigurationMapValue());

        return new GenericRow(Metadata.FULL_SCHEMA, metadataMap);
    }

    /**
     * Helper method to load the partition column names.
     */
    private Set<String> loadPartitionColNames() {
        ColumnVector partitionColNameVector = partitionColumns.getElements();
        Set<String> partitionColumnNames = new HashSet<>();
        for (int i = 0; i < partitionColumns.getSize(); i++) {
            checkArgument(!partitionColNameVector.isNullAt(i),
                "Expected a non-null partition column name");
            String partitionColName = partitionColNameVector.getString(i);
            checkArgument(partitionColName != null && !partitionColName.isEmpty(),
                "Expected non-null and non-empty partition column name");
            partitionColumnNames.add(partitionColName.toLowerCase(Locale.ROOT));
        }
        return Collections.unmodifiableSet(partitionColumnNames);
    }
}
