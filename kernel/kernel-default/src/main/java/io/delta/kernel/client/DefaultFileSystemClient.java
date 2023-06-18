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
package io.delta.kernel.client;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

public class DefaultFileSystemClient
    implements FileSystemClient
{
    private final Configuration hadoopConf;

    public DefaultFileSystemClient(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String filePath)
    {
        try {
            Iterator<org.apache.hadoop.fs.FileStatus> iter;

            Path path = new Path(filePath);
            FileSystem fs = path.getFileSystem(hadoopConf);
            if (!fs.exists(path.getParent())) {
                throw new FileNotFoundException(
                    String.format("No such file or directory: %s", path.getParent())
                );
            }
            org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(path.getParent());
            iter = Arrays.stream(files)
                .filter(f -> f.getPath().getName().compareTo(path.getName()) >= 0)
                .sorted(Comparator.comparing(o -> o.getPath().getName()))
                .iterator();

            return Utils.toCloseableIterator(iter)
                .map(hadoopFileStatus ->
                    FileStatus.of(
                        hadoopFileStatus.getPath().toString(),
                        hadoopFileStatus.getLen(),
                        hadoopFileStatus.getModificationTime())
                );
        }
        catch (Exception ex) {
            throw new RuntimeException("Could not resolve the FileSystem", ex);
        }
    }
}
