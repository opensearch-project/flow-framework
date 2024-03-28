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
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DynamoDBDemo {

    public static String createTable(DynamoDbClient ddb, String tableName, String key) {

        DynamoDbWaiter dbWaiter = ddb.waiter();
        CreateTableRequest request = CreateTableRequest.builder()
            .attributeDefinitions(AttributeDefinition.builder().attributeName(key).attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName(key).keyType(KeyType.HASH).build())
            .provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(Long.valueOf(5)).writeCapacityUnits(Long.valueOf(5)).build()
            )
            .tableName(tableName)
            .build();

        String newTable = "";
        try {
            CreateTableResponse response = ddb.createTable(request);
            DescribeTableRequest tableRequest = DescribeTableRequest.builder().tableName(tableName).build();

            // Wait until the Amazon DynamoDB table is created
            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(System.out::println);

            newTable = response.tableDescription().tableName();
            return newTable;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    public static void putItemInTable(
        DynamoDbClient ddb,
        String tableName,
        String key,
        String keyVal,
        String albumTitle,
        String albumTitleValue,
        String awards,
        String awardVal,
        String songTitle,
        String songTitleVal
    ) {

        HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();

        // Add all content to the table
        itemValues.put(key, AttributeValue.builder().s(keyVal).build());
        itemValues.put(songTitle, AttributeValue.builder().s(songTitleVal).build());
        itemValues.put(albumTitle, AttributeValue.builder().s(albumTitleValue).build());
        itemValues.put(awards, AttributeValue.builder().s(awardVal).build());

        PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

        try {
            ddb.putItem(request);
        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void getDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {

        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();

        keyToGet.put(key, AttributeValue.builder().s(keyVal).build());

        GetItemRequest request = GetItemRequest.builder().key(keyToGet).tableName(tableName).consistentRead(true).build();

        try {
            Map<String, AttributeValue> returnedItem = ddb.getItem(request).item();

            if (returnedItem.size() != 0) {
                Set<String> keys = returnedItem.keySet();
                for (String key1 : keys) {
                    System.out.format("%s: %s\n", key1, returnedItem.get(key1).s());
                }
            } else {
                System.out.format("No item found with the key: %s!\n", keyToGet.get(key).s());
            }
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void deleteDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {

        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();

        keyToGet.put(key, AttributeValue.builder().s(keyVal).build());

        DeleteItemRequest deleteReq = DeleteItemRequest.builder().tableName(tableName).key(keyToGet).build();

        try {
            ddb.deleteItem(deleteReq);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {

        try {
            String port = "8000";
            String uri = "http://localhost:" + port;

            // Create a client and connect to DynamoDB Local
            // Note: This is a dummy key and secret and AWS_ACCESS_KEY_ID can contain only letters (A–Z, a–z) and numbers (0–9).
            DynamoDbClient ddbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create(uri))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummyKey", "dummySecret")))
                .build();

            String tableName = "Music";
            String keyName = "Artist";

            // Create a table in DynamoDB Local with table name Music and partition key Artist
            // Understanding core components of DynamoDB:
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
            createTable(ddbClient, tableName, keyName);

            // List all the tables in DynamoDB Local

            System.out.println("Listing tables in DynamoDB Local...");
            System.out.println("-------------------------------");
            ListTablesResponse listTablesResponse = ddbClient.listTables();
            System.out.println(listTablesResponse.tableNames());

            String key1 = "No One you know";
            String key2 = "The Beatles";

            // Insert data into the table
            System.out.println();
            System.out.println("Inserting data into the table:" + tableName);
            System.out.println();
            putItemInTable(
                ddbClient,
                tableName,
                keyName,
                key1,
                "albumTitle",
                "The Colour And The Shape",
                "awards",
                "awardVal",
                "songTitle",
                "songTitleVal"
            );
            putItemInTable(
                ddbClient,
                tableName,
                keyName,
                key2,
                "albumTitle",
                "Let It Be",
                "awards",
                "awardVal",
                "songTitle",
                "songTitleVal"
            );

            // Get data from the table
            System.out.println("Getting Item from the table for key: " + key1);
            System.out.println("-------------------------------");
            getDynamoDBItem(ddbClient, tableName, keyName, key1);

            System.out.println();

            System.out.println("Getting Item from the table for key: " + key2);
            System.out.println("-------------------------------");
            getDynamoDBItem(ddbClient, tableName, keyName, key2);

            System.out.println();
            System.out.println("Deleting Item with key: " + key1);
            System.out.println();

            deleteDynamoDBItem(ddbClient, tableName, keyName, key1);

            System.out.println("Getting Item for key: " + key1);
            System.out.println("-------------------------------");
            getDynamoDBItem(ddbClient, tableName, keyName, key1);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
