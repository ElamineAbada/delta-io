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
package io.delta.kernel.internal.replay;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.fs.Path;

/** Internal wrapper class holding information needed to perform log replay. */
class ActionWrapper {
    private final ColumnarBatch columnarBatch;
    private final Path checkpointPath;
    private final long version;

    ActionWrapper(ColumnarBatch data, Path checkpointPath, long version) {
        this.columnarBatch = data;
        this.checkpointPath = checkpointPath;
        this.version = version;
    }

    public ColumnarBatch getColumnarBatch() {
        return columnarBatch;
    }

    public boolean isFromCheckpoint() {
        return checkpointPath != null;
    }

    public long getVersion() {
        return version;
    }

    public Path getCheckpointPath() {
        return checkpointPath;
    }
}
