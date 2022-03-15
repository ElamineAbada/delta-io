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

import org.apache.hadoop.fs.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.auth.AWSCredentialsProvider;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamoDBLogStore extends BaseExternalLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBLogStore.class);

    private AmazonDynamoDBClient client = null;
    private String tableName;
    private String credentialsProviderName;
    private String regionName;
    private String endpoint;

    public DynamoDBLogStore(Configuration hadoopConf) {
        super(hadoopConf);
        tableName = getParam(hadoopConf, "tableName", "delta_log");
        credentialsProviderName = getParam(
            hadoopConf,
            "credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        );
        regionName = getParam(hadoopConf, "region", "us-east-1");
        endpoint = getParam(hadoopConf, "endpoint", null);
    }

   /*
    * Write to db in exclusive way.
    * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
    */
    @Override
    protected void putExternalEntry(
        ExternalCommitEntry entry, boolean overwrite
    ) throws IOException {
        AmazonDynamoDBClient client = getClient();
        try {
            LOG.debug(String.format("putItem %s, overwrite: %s", entry, overwrite));
            client.putItem(createPutItemRequest(entry, tableName, overwrite));
        } catch (ConditionalCheckFailedException e) {
            LOG.debug(e.toString());
            throw new java.nio.file.FileAlreadyExistsException(
                entry.absoluteJsonPath().toString()
            );
        }
    }

    @Override
    protected ExternalCommitEntry getExternalEntry(
        Path absoluteTablePath, Path absoluteJsonPath
    ) throws IOException {
        AmazonDynamoDBClient client = getClient();
        Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put("tablePath", new AttributeValue(absoluteTablePath.toString()));
        attributes.put("fileName", new AttributeValue(absoluteJsonPath.toString()));

        java.util.Map<String, AttributeValue> item = client.getItem(
            new GetItemRequest(tableName, attributes)
               .withConsistentRead(true)
        ).getItem();
        return item != null ? itemToDbEntry(item) : null;
    }

    @Override
    protected ExternalCommitEntry getLatestExternalEntry(Path tablePath) throws IOException {
        AmazonDynamoDBClient client = getClient();
        Map<String, Condition> conditions = new ConcurrentHashMap<>();
        conditions.put(
            "tablePath",
            new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(tablePath.toString()))
        );

        try {
            java.util.Map<String, AttributeValue> item = client.query(
                new QueryRequest(tableName)
                    .withConsistentRead(true)
                    .withScanIndexForward(false)
                    .withLimit(1)
                    .withKeyConditions(conditions)
            ).getItems().get(0);
            return itemToDbEntry(item);
        } catch(IndexOutOfBoundsException e) {
            return null;
        }
    }

    private ExternalCommitEntry itemToDbEntry(java.util.Map<String, AttributeValue> item) {
        String commitTime = item.get("commitTime").getN();
        return new ExternalCommitEntry(
            new Path(item.get("tablePath").getS()),
            item.get("fileName").getS(),
            item.get("tempPath").getS(),
            item.get("complete").getS() == "true",
            commitTime != null ? Long.parseLong(commitTime) : null
        );
    }

    private PutItemRequest createPutItemRequest(
        ExternalCommitEntry entry, String tableName, boolean overwrite
    ) {

        Map<String, AttributeValue> attributes = new ConcurrentHashMap<>();
        attributes.put("tablePath", new AttributeValue(entry.tablePath.toString()));
        attributes.put("fileName", new AttributeValue(entry.fileName));
        attributes.put("tempPath", new AttributeValue(entry.tempPath));
        attributes.put(
            "complete",
            new AttributeValue().withS(new Boolean(entry.complete).toString())
        );
        if (entry.complete) {
            attributes.put("commitTime", new AttributeValue().withN(entry.commitTime.toString()));
        }
        PutItemRequest pr = new PutItemRequest(tableName, attributes);
        if(!overwrite) {
            Map<String, ExpectedAttributeValue> expected = new ConcurrentHashMap<>();
            expected.put("fileName", new ExpectedAttributeValue(false));
            pr.withExpected(expected);
        }
        return pr;
    }

    AmazonDynamoDBClient getClient() throws java.io.IOException {
        Configuration config = initHadoopConf();
        if(client == null) {
            // TODO - maybe use some helper?
            try {
                AWSCredentialsProvider auth =
                    (AWSCredentialsProvider)Class.forName(credentialsProviderName)
                    .getConstructor().newInstance();
                client = new AmazonDynamoDBClient(auth);
                client.setRegion(Region.getRegion(Regions.fromName(regionName)));
                if(endpoint != null) {
                    client.setEndpoint(endpoint);
                }
            } catch(
                ClassNotFoundException
                | InstantiationException
                | NoSuchMethodException
                | IllegalAccessException
                | java.lang.reflect.InvocationTargetException e
            ) {
                throw new java.io.IOException(e);
            }
        }
        return client;
    }

    protected String getParam(Configuration config, String name, String defaultValue) {
        return config.get(
            String.format("hadoop.delta.DynamoDBLogStore.%s", name),  // TODO
            defaultValue
        );
    }
}
