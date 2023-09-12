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
package io.delta.kernel.defaults.client;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.data.Row;

/**
 * Default implementation of {@link FileReadContext}.
 *
 * @see FileReadContext
 */
public class DefaultFileReadContext
    implements FileReadContext {
    private final Row scanFileRow;

    public DefaultFileReadContext(Row scanFileRow) {
        this.scanFileRow = requireNonNull(scanFileRow, "scanFileRow is null");
    }

    @Override
    public Row getScanFileRow() {
        return this.scanFileRow;
    }
}
