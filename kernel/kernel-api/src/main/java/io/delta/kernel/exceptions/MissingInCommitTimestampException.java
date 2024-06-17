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
package io.delta.kernel.exceptions;

import io.delta.kernel.annotation.Evolving;

/**
 * Thrown when commitTimestamp field has not been set in.
 *
 * @since 3.2.0
 */
@Evolving
public class MissingInCommitTimestampException extends IllegalStateException {
    private final String featureName;
    private final String commitVersion;

    public MissingInCommitTimestampException(String featureName, String commitVersion) {
        this.featureName = featureName;
        this.commitVersion = commitVersion;
    }

    @Override
    public String getMessage() {
        return String.format(
                "This table has the feature %s enabled which requires the presence of " +
                "commitTimestamp in the CommitInfo action. However, this field has not been set " +
                "in commit version %s.",
                featureName,
                commitVersion);
    }
}
