/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import io.delta.storage.HadoopFileSystemLogStore;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.collections.iterators.FilterIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Used so that we can use an external store child implementation
* to provide the mutual exclusion that the cloud store,
* e.g. s3, is missing.
*/
public abstract class BaseExternalLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(BaseExternalLogStore.class);

    ////////////////////////
    // Public API Methods //
    ////////////////////////

    public BaseExternalLogStore(Configuration hadoopConf) {
        super(hadoopConf);
        deltaFilePattern = Pattern.compile("\\d+\\.json");
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = stripUserInfo(fs.makeQualified(path));
        final Path tablePath = getTablePath(resolvedPath);
        final Optional<ExternalCommitEntry> entry = getLatestExternalEntry(tablePath);
        if (entry.isPresent()) {
            fixDeltaLog(fs, entry.get());
        }

        // This is predicated on the storage system providing consistent listing
        // If there was a recovery performed in the `fixDeltaLog` call, then some temp file
        // was just copied into some N.json in the delta log. Because of consistent listing,
        // the `super.listFrom` is guaranteed to see N.json.
        return super.listFrom(path, hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = stripUserInfo(fs.makeQualified(path));

        if (overwrite) {
            writeActions(fs, path, actions);
            return;
        }

        final Path tablePath = getTablePath(resolvedPath);
        if (isDeltaFile(path)) {
            final long version = deltaVersion(path);
            if (version > 0) {
                final long prevVersion = version - 1;
                final Path prevPath = deltaFile(tablePath, prevVersion);
                final Optional<ExternalCommitEntry> entry = getExternalEntry(tablePath, prevPath);
                if (entry.isPresent()) {
                    fixDeltaLog(fs, entry.get());
                } else {
                    if (!fs.exists(prevPath)) {
                        throw new java.nio.file.FileSystemException(
                            String.format("previous commit %s doesn't exist", prevPath)
                        );
                    }
                }
            } else {
                final Optional<ExternalCommitEntry> entry = getExternalEntry(tablePath, path);
                if (entry.isPresent()) {
                    if (entry.get().complete && !fs.exists(path)) {
                        throw new java.nio.file.FileSystemException(
                            String.format(
                                "Old entries for %s still exist in the database", tablePath
                            )
                        );
                    }
                }
            }
        }

        final String tempPath = createTemporaryPath(resolvedPath);
        final ExternalCommitEntry entry = new ExternalCommitEntry(
            tablePath,
            resolvedPath.getName(),
            tempPath,
            false, // not complete
            null // commitTime
        );

        writeActions(fs, entry.absoluteTempPath(), actions);
        putExternalEntry(entry, false); // overwrite=false

        try {
            writeCopyTempFile(fs, entry.absoluteTempPath(), resolvedPath);
            writePutCompleteDbEntry(entry);
        } catch (Throwable e) {
            LOG.info(
                "{}: ignoring recoverable error: {}", e.getClass().getSimpleName(), e
            );
        }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }

    /////////////////////////////////////////////////////////////
    // Protected Members (for interaction with external store) //
    /////////////////////////////////////////////////////////////

    /**
     * Write file with actions under a specific path.
     */
    protected void writeActions(
        FileSystem fs,
        Path path,
        Iterator<String> actions
    ) throws IOException {
        LOG.debug("writeActions to: {}", path);
        FSDataOutputStream stream = fs.create(path, true);
        while(actions.hasNext()) {
            byte[] line = String.format("%s\n", actions.next()).getBytes(StandardCharsets.UTF_8);
            stream.write(line);
        }
        stream.close();
    }

    /**
     * Generate temporary path for TransactionLog.
     */
    protected String createTemporaryPath(Path path) {
        String uuid = java.util.UUID.randomUUID().toString();
        return String.format(".tmp/%s.%s", path.getName(), uuid);
    }

    /**
     * Returns the base table path for a given Delta log entry located in
     * e.g. input path of $tablePath/_delta_log/00000N.json would return $tablePath
     */
    protected Path getTablePath(Path path) {
        return path.getParent().getParent();
    }

    /**
     * Write to external store in exclusive way.
     *
     * @throws java.nio.file.FileAlreadyExistsException if path exists in cache and `overwrite` is
     *                                                  false
     */
    abstract protected void putExternalEntry(
        ExternalCommitEntry entry,
        boolean overwrite) throws IOException;

    /**
     * Return external store entry corresponding to delta log file with given `tablePath` and
     * `fileName`, or `Optional.empty()` if it doesn't exist.
     *
     * @param tablePath TODO
     * @param jsonPath TODO
     */
    abstract protected Optional<ExternalCommitEntry> getExternalEntry(
        Path tablePath,
        Path jsonPath) throws IOException;

    /**
     * Return the latest external store entry corresponding to the delta log for given `tablePath`,
     * or `Optional.empty()` if it doesn't exist.
     *
     * @param tablePath TODO
     */
    abstract protected Optional<ExternalCommitEntry> getLatestExternalEntry(
        Path tablePath) throws IOException;

    //////////////////////////////////////////////////////////
    // Protected Members (for error injection during tests) //
    //////////////////////////////////////////////////////////

    /**
     * Wrapper for `copyFile`, called by the `write` method.
     */
    protected void writeCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    /**
     * Wrapper for `putExternalEntry`, called by the `write` method.
     */
    protected void writePutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    /**
     * Wrapper for `copyFile`, called by the `fixDeltaLog` method.
     */
    protected void fixDeltaLogCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    /**
     * Wrapper for `putExternalEntry`, called by the `fixDeltaLog` method.
     */
    protected void fixDeltaLogPutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    /**
     * Method for assuring consistency on filesystem according to the external cache.
     * Method tries to rewrite TransactionLog entry from temporary path if it does not exist.
     */
    private void fixDeltaLog(FileSystem fs, ExternalCommitEntry entry) throws IOException {
        if (entry.complete) {
            return;
        }
        int retry = 0;
        boolean copied = false;
        while (true) {
            LOG.debug("trying to fix: {}", entry.fileName);
            try {
                if (!copied && !fs.exists(entry.absoluteJsonPath())) {
                    fixDeltaLogCopyTempFile(fs, entry.absoluteTempPath(), entry.absoluteJsonPath());
                    copied = true;
                }
                fixDeltaLogPutCompleteDbEntry(entry);
                LOG.info("fixed {}", entry.fileName);
                return;
            } catch(Throwable e) {
                LOG.info("{}: {}", e.getClass().getSimpleName(), e);
                if (retry >= 3) {
                    throw e;
                }
            }
            retry += 1;
        }
    }

   /**
    * Copies file within filesystem
    * @param fs reference to [[FileSystem]]
    * @param src path to source file
    * @param dst path to destination file
    */
    private void copyFile(FileSystem fs, Path src, Path dst) throws IOException {
        LOG.debug("copy file: {} -> {}", src, dst);
        FSDataInputStream input_stream = fs.open(src);
        FSDataOutputStream output_stream = fs.create(dst, true);
        try {
            IOUtils.copy(input_stream, output_stream);
            output_stream.close();
        } finally {
            input_stream.close();
        }
    }

    /**
     * Returns path stripped user info.
     */
    private Path stripUserInfo(Path path) {
        final URI uri = path.toUri();

        try {
            final URI newUri = new URI(
                uri.getScheme(),
                null, // userInfo
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );

            return new Path(newUri);
        } catch (URISyntaxException e) {
            // Propagating this URISyntaxException to callers would mean we would have to either
            // include it in the public LogStore.java interface or wrap it in an
            // IllegalArgumentException somewhere else. Instead, catch and wrap it here.
            throw new IllegalArgumentException(e);
        }
    }

    // TODO - use java version of FileNames utils?
    private Pattern deltaFilePattern;

    private boolean isDeltaFile(Path path) {
        return deltaFilePattern.matcher(path.getName()).matches();
    }

    private Path deltaFile(Path tablePath, long version) {
        return new Path(tablePath, String.format("_delta_log/%020d.json", version));
    }

    private long deltaVersion(Path path) {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

}
