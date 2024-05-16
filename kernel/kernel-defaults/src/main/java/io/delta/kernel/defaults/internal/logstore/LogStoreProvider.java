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
package io.delta.kernel.defaults.internal.logstore;

import java.util.*;

import io.delta.storage.*;
import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.defaults.internal.DefaultEngineErrors;

/**
 * Utility class to provide the correct {@link LogStore} based on the scheme of the path.
 */
public class LogStoreProvider {

    // Supported schemes per storage system.
    private static final Set<String> S3_SCHEMES = unmodifiableSet("s3", "s3a", "s3n");
    private static final Set<String> AZURE_SCHEMES =
            unmodifiableSet("abfs", "abfss", "adl", "wasb", "wasbs");
    private static final Set<String> GCS_SCHEMES = unmodifiableSet("gs");

    /**
     * Get the {@link LogStore} instance for the given scheme and configuration. Callers can set
     * {@code io.delta.kernel.logStore.<scheme>.impl} to specify the {@link LogStore}
     * implementation to use for {@code scheme}.
     * <p>
     * If not set, the default {@link LogStore} implementation (given below) for the scheme will
     * be used.
     * <ul>
     *     <li>{@code s3, s3a, s3n}: {@link S3SingleDriverLogStore}</li>
     *     <li>{@code abfs, abfss, adl, wasb, wasbs}: {@link AzureLogStore}</li>
     *     <li>{@code gs}: {@link GCSLogStore}</li>
     *     <li>{@code hdfs, file}: {@link HDFSLogStore}</li>
     *     <li>remaining: {@link HDFSLogStore}</li>
     * </ul>
     *
     * @param hadoopConf {@link Configuration} to use for creating the LogStore.
     * @param scheme     Scheme of the path.
     * @return {@link LogStore} instance.
     * @throws IllegalArgumentException if the LogStore implementation is not found or can not be
     *                                  instantiated.
     */
    public static LogStore getLogStore(Configuration hadoopConf, String scheme) {
        String schemeLower = Optional.ofNullable(scheme)
                .map(String::toLowerCase).orElse(null);

        // Check if the LogStore implementation is set in the configuration.
        String classNameFromConfig = hadoopConf.get(getLogStoreSchemeConfKey(schemeLower));
        if (classNameFromConfig != null) {
            try {
                return getLogStoreClass(classNameFromConfig)
                        .getConstructor(Configuration.class)
                        .newInstance(hadoopConf);
            } catch (Exception e) {
                throw DefaultEngineErrors.canNotInstantiateLogStore(classNameFromConfig);
            }
        }

        // Create default LogStore based on the scheme.
        String defaultClassName = HDFSLogStore.class.getName();
        if (S3_SCHEMES.contains(schemeLower)) {
            defaultClassName = S3SingleDriverLogStore.class.getName();
        } else if (AZURE_SCHEMES.contains(schemeLower)) {
            defaultClassName = AzureLogStore.class.getName();
        } else if (GCS_SCHEMES.contains(schemeLower)) {
            defaultClassName = GCSLogStore.class.getName();
        }

        try {
            return getLogStoreClass(defaultClassName)
                    .getConstructor(Configuration.class)
                    .newInstance(hadoopConf);
        } catch (Exception e) {
            throw DefaultEngineErrors.canNotInstantiateLogStore(defaultClassName);
        }
    }

    /**
     * Configuration key for setting the LogStore implementation for a scheme.
     * ex: `io.delta.kernel.logStore.s3.impl` -> `io.delta.storage.S3SingleDriverLogStore`
     */
    static String getLogStoreSchemeConfKey(String scheme) {
        return "io.delta.kernel.logStore." + scheme + ".impl";
    }

    /**
     * Utility method to get the LogStore class from the class name.
     */
    private static Class<? extends LogStore> getLogStoreClass(String logStoreClassName)
            throws ClassNotFoundException {
        return Class.forName(
                        logStoreClassName,
                        true /* initialize */,
                        Thread.currentThread().getContextClassLoader())
                .asSubclass(LogStore.class);
    }

    /**
     * Remove this method once we start supporting JDK9+
     */
    private static Set<String> unmodifiableSet(String... elements) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(elements)));
    }
}
