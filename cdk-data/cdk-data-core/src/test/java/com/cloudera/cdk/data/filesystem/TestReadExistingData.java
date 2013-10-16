/**
 * Copyright 2013 Cloudera Inc.
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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetNotFoundException;
import com.cloudera.cdk.data.DatasetRepositories;
import com.cloudera.cdk.data.DatasetRepository;
import com.google.common.io.Files;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.USER_SCHEMA;
import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.checkTestUsers;

public class TestReadExistingData {

  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
    testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
  }

  private void writeTestUsers(Path file, int count) throws IOException {
    GenericDatumWriter<GenericRecord> writer =
        new GenericDatumWriter<GenericRecord>(USER_SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(USER_SCHEMA, fileSystem.create(file));
    for (int i = 0; i < count; i++) {
      GenericData.Record record = new GenericRecordBuilder(USER_SCHEMA)
          .set("username", "test-" + i)
          .set("email", "email-" + i).build();
      dataFileWriter.append(record);
    }
    dataFileWriter.close();
  }

  @Test(expected = DatasetNotFoundException.class)
  public void testReadAvroFileAsDataset() throws IOException {
    writeTestUsers(new Path(testDirectory, "file.avro"), 10);
    DatasetRepository repo = DatasetRepositories.open("repo:" + testDirectory.getParent().toUri());
    // Following line fails since there is no .metadata directory
    Dataset<GenericData.Record> dataset = repo.load(testDirectory.getName());
    checkTestUsers(dataset, 10);
  }

  @Test
  public void testConvertAvroFileToDataset() throws IOException {
    Path file = new Path(testDirectory, "file.avro");
    writeTestUsers(file, 10);
    DatasetRepository repo = DatasetRepositories.open("repo:" + testDirectory.getParent().toUri());
    // Call create method to ensure .metadata is created
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schemaFromAvroDataFile(fileSystem.open(file)).build();
    Dataset<GenericData.Record> dataset = repo.create(testDirectory.getName(), descriptor);
    checkTestUsers(dataset, 10);
  }

}
