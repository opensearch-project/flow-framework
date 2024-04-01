/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import org.opensearch.ExceptionsHelper;
import org.opensearch.SpecialPermission;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.index.get.GetResult;

import java.io.IOException;
import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

/**
 * Provides utilities and wrapper methods for using DynamoDB as a metadata store.
 */
public class DynamoDbUtil {

    /** A boolean switch simply wrapping the NodeClient implementation */
    public static final boolean USE_DYNAMODB = true;

    /** The DynamoDbClient, only instantiated if we're using it */
    private static final DynamoDbClient DDB = USE_DYNAMODB
        // Really this instantiation belongs in createComponents and injected here
        // But how are we handling injection in serverless?
        ? SocketAccess.doPrivileged(
            (PrivilegedAction<DynamoDbClient>) () -> DynamoDbClient.builder()
                .overrideConfiguration(ClientOverrideConfiguration.builder().build())
                .endpointOverride(URI.create("http://localhost:8000"))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .region(Region.US_WEST_2)
                // Demo only, these are not real credentials anywhere
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
                .build()
        )
        : null;

    /* The primary key for indices */
    private static final String DOC_ID = "id";
    /* The key for the document source */
    private static final String DOC_SOURCE = "source";

    static {
        // Aws v2 sdk tries to load a default profile from home path which is restricted. Hence, setting these to random valid paths.
        // @SuppressForbidden(reason = "Need to provide this override to v2 SDK so that path does not default to home path")
        if (ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.getStringValue().isEmpty()) {
            SocketAccess.doPrivileged(
                () -> System.setProperty(
                    ProfileFileSystemSetting.AWS_SHARED_CREDENTIALS_FILE.property(),
                    System.getProperty("opensearch.path.conf")
                )
            );
        }
        if (ProfileFileSystemSetting.AWS_CONFIG_FILE.getStringValue().isEmpty()) {
            SocketAccess.doPrivileged(
                () -> System.setProperty(ProfileFileSystemSetting.AWS_CONFIG_FILE.property(), System.getProperty("opensearch.path.conf"))
            );
        }
    }

    private static final class SocketAccess {
        private SocketAccess() {}

        public static <T> T doPrivileged(PrivilegedAction<T> operation) {
            SpecialPermission.check();
            return AccessController.doPrivileged(operation);
        }
    }

    /**
     * A wrapper class for {@link Client} but implementing in DynamoDB.
     */
    public static class DDBClient {

        private final Client client;
        private final DDBAdminClient adminClient;

        /**
         * Instantiate this class wrapping a NodeClient.
         * @param client The NodeClient
         */
        public DDBClient(Client client) {
            this.client = client;
            this.adminClient = new DDBAdminClient(client.admin());
        }

        /**
         * Represent AdminClient.
         * @return The admin client
         */
        public DDBAdminClient admin() {
            return this.adminClient;
        }

        /**
         * Index data to an OpenSearch index or DynamoDB table
         * @param request an Index request
         * @param actionListener the listener for response or exception
         */
        public void index(IndexRequest request, ActionListener<IndexResponse> actionListener) {
            if (USE_DYNAMODB) {
                putItemInTable(request, actionListener);
            } else {
                client.index(request, actionListener);
            }
        }

        /**
         * Get data from an OpenSearch index or DynamoDB table
         * @param request a Get request
         * @param actionListener the listener for response or exception
         */
        public void get(GetRequest request, ActionListener<GetResponse> actionListener) {
            if (USE_DYNAMODB) {
                getItemFromTable(request, actionListener);
            } else {
                client.get(request, actionListener);
            }
        }

        /**
         * Delete data from an OpenSearch index or DynamoDB table
         * @param request a Delete request
         * @param actionListener the listener for response or exception
         */
        public void delete(DeleteRequest request, ActionListener<DeleteResponse> actionListener) {
            if (USE_DYNAMODB) {
                deleteItemFromTable(request, actionListener);
            } else {
                client.delete(request, actionListener);
            }
        }
    }

    /**
     * A wrapper class for AdminClient
     */
    public static class DDBAdminClient {

        private final DDBIndicesAdminClient indicesAdminClient;

        /**
         * Instantiate this class using an AdminClient
         * @param adminClient The admin client
         */
        public DDBAdminClient(AdminClient adminClient) {
            this.indicesAdminClient = new DDBIndicesAdminClient(adminClient.indices());
        }

        /**
         * Represent IndicesAdminClient.
         * @return The indices client
         */
        public DDBIndicesAdminClient indices() {
            return this.indicesAdminClient;
        }
    }

    /**
     * A wrapper class for IndicesAdminClient
     */
    public static class DDBIndicesAdminClient {

        private final IndicesAdminClient indicesAdminClient;

        /**
         * Instantiate this class using an IndicesAdminClient
         * @param indicesAdminClient The indices admin client
         */
        public DDBIndicesAdminClient(IndicesAdminClient indicesAdminClient) {
            this.indicesAdminClient = indicesAdminClient;
        }

        /**
         * Create an OpenSearch index or DynamoDB table.
         * @param request The Create Index Request
         * @param actionListener the listener for response or exception
         */
        public void create(CreateIndexRequest request, ActionListener<CreateIndexResponse> actionListener) {
            if (USE_DYNAMODB) {
                createTable(request, actionListener);
            } else {
                indicesAdminClient.create(request, actionListener);
            }
        }

    }

    /*
     * Utility methods for table access.
     */

    /**
     * Test whether a table exists.
     * @param tableName The table name
     * @return true if the table exists
     */
    public static boolean tableExists(String tableName) {
        try {
            SocketAccess.doPrivileged(() -> DDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build()));
            // If no exception is thrown, the table exists
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    /**
     * Count the items in a table.
     * @param tableName The table name
     * @return the number of items in the table
     */
    public static long itemCount(String tableName) {
        try {
            DescribeTableResponse response = SocketAccess.doPrivileged(
                () -> DDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build())
            );
            return response.table().itemCount();
        } catch (ResourceNotFoundException e) {
            return 0;
        }
    }

    /**
     * Create a new table representing an OpenSearch index
     * @param createIndexRequest the create index request
     * @param actionListener the listener to return response or exception
     */
    public static void createTable(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> actionListener) {
        String tableName = createIndexRequest.index();
        DynamoDbWaiter dbWaiter = DDB.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
            .attributeDefinitions(AttributeDefinition.builder().attributeName(DOC_ID).attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName(DOC_ID).keyType(KeyType.HASH).build())
            .provisionedThroughput(pt -> pt.readCapacityUnits(Long.valueOf(5)).writeCapacityUnits(Long.valueOf(5)))
            .tableName(tableName)
            .build();
        try {
            CreateTableResponse response = SocketAccess.doPrivileged(() -> DDB.createTable(request));
            DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(tableName).build();

            WaiterResponse<DescribeTableResponse> waiterResponse = SocketAccess.doPrivileged(
                () -> dbWaiter.waitUntilTableExists(tableRequest)
            );
            ResponseOrException<DescribeTableResponse> responseOrException = waiterResponse.matched();
            if (responseOrException.response().isPresent()) {
                actionListener.onResponse(new CreateIndexResponse(true, true, response.tableDescription().tableName()));
            } else {
                actionListener.onFailure(
                    new FlowFrameworkException(
                        "Failed to create table " + tableName,
                        ExceptionsHelper.status(responseOrException.exception().orElse(null))
                    )
                );
            }
        } catch (DynamoDbException e) {
            actionListener.onFailure(new FlowFrameworkException("Failed to create table " + tableName, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Index an item in a table.
     * @param indexRequest The index request
     * @param actionListener the listener to return response or exception
     */
    public static void putItemInTable(IndexRequest indexRequest, ActionListener<IndexResponse> actionListener) {
        String tableName = indexRequest.index();
        String id = indexRequest.id() == null ? UUIDs.base64UUID() : indexRequest.id();
        try {
            String source = XContentHelper.convertToJson(indexRequest.source(), false, false, indexRequest.getContentType());
            PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(
                    Map.ofEntries(
                        Map.entry(DOC_ID, AttributeValue.builder().s(id).build()),
                        Map.entry(DOC_SOURCE, AttributeValue.builder().s(source).build())
                    )
                )
                .build();
            SocketAccess.doPrivileged(() -> DDB.putItem(request));
            actionListener.onResponse(new IndexResponse(new ShardId(tableName, tableName, 0), id, 0, 0, 0, true));
        } catch (IOException e) {
            actionListener.onFailure(
                new FlowFrameworkException("Unable to parse source to insert in table " + tableName, RestStatus.BAD_REQUEST)
            );
        } catch (ResourceNotFoundException e) {
            actionListener.onFailure(new FlowFrameworkException("The table " + tableName + " does not exist", RestStatus.NOT_FOUND));
        } catch (DynamoDbException e) {
            actionListener.onFailure(
                new FlowFrameworkException("Failed to put item " + id + " in table " + tableName, ExceptionsHelper.status(e))
            );
        }
    }

    /**
     * Get an item from a table.
     * @param getRequest The get request
     * @param actionListener the listener to return response or exception
     */
    public static void getItemFromTable(GetRequest getRequest, ActionListener<GetResponse> actionListener) {
        String tableName = getRequest.index();
        String id = getRequest.id();
        GetItemRequest request = GetItemRequest.builder()
            .key(Map.of(DOC_ID, AttributeValue.builder().s(id).build()))
            .tableName(tableName)
            .consistentRead(true)
            .build();
        GetResult result;
        try {
            Map<String, AttributeValue> item = SocketAccess.doPrivileged(() -> DDB.getItem(request).item());
            result = item.containsKey(DOC_SOURCE)
                ? new GetResult(tableName, id, 0, 0, 0, true, new BytesArray(item.get(DOC_SOURCE).s()), null, null)
                : new GetResult(tableName, id, 0, 0, 0, false, null, null, null);
        } catch (DynamoDbException e) {
            result = new GetResult(tableName, id, 0, 0, 0, false, null, null, null);
        }
        actionListener.onResponse(new GetResponse(result));
    }

    /**
     * Delete an item from a table.
     * @param deleteRequest The delete request
     * @param actionListener the listener to return response or exception
     */
    public static void deleteItemFromTable(DeleteRequest deleteRequest, ActionListener<DeleteResponse> actionListener) {
        String tableName = deleteRequest.index();
        String id = deleteRequest.id();
        DeleteItemRequest request = DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(DOC_ID, AttributeValue.builder().s(id).build()))
            .build();
        ShardId shardId = new ShardId(tableName, tableName, 0);
        try {
            DeleteItemResponse result = SocketAccess.doPrivileged(() -> DDB.deleteItem(request));
            boolean found = result.attributes() != null && !result.attributes().isEmpty();
            actionListener.onResponse(new DeleteResponse(shardId, id, 0, 0, 0, found));
        } catch (DynamoDbException e) {
            actionListener.onResponse(new DeleteResponse(shardId, id, 0, 0, 0, false));
        }
    }
}
