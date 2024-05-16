/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.kernel.engine.Engine;

/**
 * Throws when the {@link Engine} encountered an error while executing an operation.
 */
public class KernelEngineException extends RuntimeException {
    private static final String msgT = "Encountered an error from the underlying engine " +
            "implementation while trying to %s: %s";

    public KernelEngineException(String attemptedOperation, Throwable cause) {
        super(String.format(msgT, attemptedOperation, cause.getMessage()), cause);
    }
}
