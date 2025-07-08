/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link JsonToJsonTransformer}.
 */
public class JsonToJsonTransformerTests extends OpenSearchTestCase {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void testSimplePropertyMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "user": {
                "firstName": "John",
                "lastName": "Doe",
                "age": 30
              }
            }
            """;

        String mappingJson = """
            {
              "name": "$.user.firstName",
              "surname": "$.user.lastName",
              "years": "$.user.age"
            }
            """;

        String expectedOutput = """
            {
              "name": "John",
              "surname": "Doe",
              "years": 30
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testNestedObjectMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "company": {
                "name": "TechCorp",
                "location": {
                  "city": "Seattle",
                  "state": "WA"
                }
              }
            }
            """;

        String mappingJson = """
            {
              "companyName": "$.company.name",
              "address": {
                "city": "$.company.location.city",
                "state": "$.company.location.state"
              }
            }
            """;

        String expectedOutput = """
            {
              "companyName": "TechCorp",
              "address": {
                "city": "Seattle",
                "state": "WA"
              }
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testArrayWithWildcardMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "item": {
                "name": "Widget",
                "allocDetails": [
                  {
                    "useful": {
                      "items": [
                        {
                          "allocation": 10,
                          "team": { "id": "T1", "name": "Alpha" }
                        },
                        {
                          "allocation": 20,
                          "team": { "id": "T2", "name": "Beta" }
                        },
                        {
                          "allocation": 30,
                          "team": { "id": "T3", "name": "Gamma" }
                        }
                      ]
                    }
                  }
                ]
              }
            }
            """;

        String mappingJson = """
            {
              "name": "$.item.name",
              "allocDetails[*]": {
                "allocation": "$.item.allocDetails[0].useful.items[*].allocation",
                "team": {
                  "id": "$.item.allocDetails[0].useful.items[*].team.id",
                  "name": "$.item.allocDetails[0].useful.items[*].team.name"
                }
              }
            }
            """;

        String expectedOutput = """
            {
              "name": "Widget",
              "allocDetails": [
                {
                  "allocation": 10,
                  "team": {
                    "id": "T1",
                    "name": "Alpha"
                  }
                },
                {
                  "allocation": 20,
                  "team": {
                    "id": "T2",
                    "name": "Beta"
                  }
                },
                {
                  "allocation": 30,
                  "team": {
                    "id": "T3",
                    "name": "Gamma"
                  }
                }
              ]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testArrayWithExplicitIndexMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "products": [
                {"name": "Product A", "price": 100},
                {"name": "Product B", "price": 200},
                {"name": "Product C", "price": 300}
              ]
            }
            """;

        String mappingJson = """
            {
              "items[0]": {
                "productName": "$.products[0].name",
                "cost": "$.products[0].price"
              },
              "items[2]": {
                "productName": "$.products[2].name",
                "cost": "$.products[2].price"
              }
            }
            """;

        String expectedOutput = """
            {
              "items": [
                {
                  "productName": "Product A",
                  "cost": 100
                },
                {},
                {
                  "productName": "Product C",
                  "cost": 300
                }
              ]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testMixedArrayAndObjectMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "order": {
                "id": "ORD123",
                "customer": {
                  "name": "Jane Smith",
                  "email": "jane@example.com"
                },
                "items": [
                  {"sku": "SKU001", "quantity": 2, "price": 50.00},
                  {"sku": "SKU002", "quantity": 1, "price": 75.00}
                ]
              }
            }
            """;

        String mappingJson = """
            {
              "orderId": "$.order.id",
              "customerInfo": {
                "fullName": "$.order.customer.name",
                "contactEmail": "$.order.customer.email"
              },
              "orderItems[*]": {
                "productSku": "$.order.items[*].sku",
                "qty": "$.order.items[*].quantity",
                "unitPrice": "$.order.items[*].price"
              }
            }
            """;

        String expectedOutput = """
            {
              "orderId": "ORD123",
              "customerInfo": {
                "fullName": "Jane Smith",
                "contactEmail": "jane@example.com"
              },
              "orderItems": [
                {
                  "productSku": "SKU001",
                  "qty": 2,
                  "unitPrice": 50.0
                },
                {
                  "productSku": "SKU002",
                  "qty": 1,
                  "unitPrice": 75.0
                }
              ]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testEmptyArrayMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "data": {
                "items": []
              }
            }
            """;

        String mappingJson = """
            {
              "results[*]": {
                "value": "$.data.items[*].value"
              }
            }
            """;

        String expectedOutput = """
            {
              "results": []
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testNonExistentPathMapping() throws JsonProcessingException {
        String inputJson = """
            {
              "user": {
                "name": "John"
              }
            }
            """;

        String mappingJson = """
            {
              "userName": "$.user.name",
              "userAge": "$.user.age",
              "userEmail": "$.user.contact.email"
            }
            """;

        String expectedOutput = """
            {
              "userName": "John"
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testMultipleValueExtraction() throws JsonProcessingException {
        String inputJson = """
            {
              "tags": ["red", "blue", "green"],
              "categories": ["electronics", "gadgets"]
            }
            """;

        String mappingJson = """
            {
              "allTags": "$.tags[*]",
              "allCategories": "$.categories[*]"
            }
            """;

        String expectedOutput = """
            {
              "allTags": ["red", "blue", "green"],
              "allCategories": ["electronics", "gadgets"]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testDeepNestedStructure() throws JsonProcessingException {
        String inputJson = """
            {
              "level1": {
                "level2": {
                  "level3": {
                    "level4": {
                      "value": "deep_value",
                      "items": [
                        {"id": 1, "data": {"info": "item1"}},
                        {"id": 2, "data": {"info": "item2"}}
                      ]
                    }
                  }
                }
              }
            }
            """;

        String mappingJson = """
            {
              "deepValue": "$.level1.level2.level3.level4.value",
              "deepItems[*]": {
                "itemId": "$.level1.level2.level3.level4.items[*].id",
                "itemInfo": "$.level1.level2.level3.level4.items[*].data.info"
              }
            }
            """;

        String expectedOutput = """
            {
              "deepValue": "deep_value",
              "deepItems": [
                {
                  "itemId": 1,
                  "itemInfo": "item1"
                },
                {
                  "itemId": 2,
                  "itemInfo": "item2"
                }
              ]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    public void testErrorHandling_NullInputJson() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonTransformer.transform(null, "{}")
        );
        assertEquals("inputJson must not be null or empty", exception.getMessage());
    }

    public void testErrorHandling_NullMappingRules() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonTransformer.transform("{}", null)
        );
        assertEquals("mappingRules must not be null or empty", exception.getMessage());
    }

    public void testErrorHandling_InvalidInputJson() {
        String invalidJson = "{ invalid json }";
        String mappingJson = "{}";

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonTransformer.transform(invalidJson, mappingJson)
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains("Invalid input JSON for JsonPath parsing"));
    }

    public void testErrorHandling_InvalidMappingJson() {
        String inputJson = "{}";
        String invalidMappingJson = "{ invalid json }";

        JsonProcessingException exception = assertThrows(
            JsonProcessingException.class,
            () -> JsonToJsonTransformer.transform(inputJson, invalidMappingJson)
        );
        assertTrue("Failed to parse mapping rules", exception.getMessage().contains("Unexpected character"));
    }

    public void testErrorHandling_TextualRootMapping() {
        String inputJson = "{}";
        String textualMapping = "\"$.root\"";

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonTransformer.transform(inputJson, textualMapping)
        );
        assertEquals("Root mapping cannot be a JsonPath string - it must be an object", exception.getMessage());
    }

    public void testErrorHandling_UnsupportedArrayIndexFormat() {
        String inputJson = """
            {
              "items": [1, 2, 3]
            }
            """;

        String mappingJson = """
            {
              "results[invalid]": {
                "value": "$.items[0]"
              }
            }
            """;

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonTransformer.transform(inputJson, mappingJson)
        );
        assertTrue(exception.getMessage().contains("Unsupported array index format"));
    }

    public void testComplexRealWorldScenario() throws JsonProcessingException {
        String inputJson = """
            {
              "response": {
                "status": "success",
                "data": {
                  "users": [
                    {
                      "id": 1,
                      "profile": {
                        "personal": {
                          "firstName": "Alice",
                          "lastName": "Johnson"
                        },
                        "contact": {
                          "emails": ["alice@work.com", "alice@personal.com"],
                          "phones": ["+1-555-0101", "+1-555-0102"]
                        }
                      },
                      "permissions": ["read", "write"]
                    },
                    {
                      "id": 2,
                      "profile": {
                        "personal": {
                          "firstName": "Bob",
                          "lastName": "Smith"
                        },
                        "contact": {
                          "emails": ["bob@work.com"],
                          "phones": ["+1-555-0201"]
                        }
                      },
                      "permissions": ["read"]
                    }
                  ]
                }
              }
            }
            """;

        String mappingJson = """
            {
              "status": "$.response.status",
              "userList[*]": {
                "userId": "$.response.data.users[*].id",
                "fullName": {
                  "first": "$.response.data.users[*].profile.personal.firstName",
                  "last": "$.response.data.users[*].profile.personal.lastName"
                },
                "contactInfo": {
                  "primaryEmail": "$.response.data.users[*].profile.contact.emails[0]",
                  "allEmails": "$.response.data.users[*].profile.contact.emails[*]",
                  "primaryPhone": "$.response.data.users[*].profile.contact.phones[0]"
                },
                "accessRights": "$.response.data.users[*].permissions[*]"
              }
            }
            """;

        String expectedOutput = """
            {
              "status": "success",
              "userList": [
                {
                  "userId": 1,
                  "fullName": {
                    "first": "Alice",
                    "last": "Johnson"
                  },
                  "contactInfo": {
                    "primaryEmail": "alice@work.com",
                    "allEmails": ["alice@work.com", "alice@personal.com"],
                    "primaryPhone": "+1-555-0101"
                  },
                  "accessRights": ["read", "write"]
                },
                {
                  "userId": 2,
                  "fullName": {
                    "first": "Bob",
                    "last": "Smith"
                  },
                  "contactInfo": {
                    "primaryEmail": "bob@work.com",
                    "allEmails": ["bob@work.com"],
                    "primaryPhone": "+1-555-0201"
                  },
                  "accessRights": ["read"]
                }
              ]
            }
            """;

        String actualOutput = JsonToJsonTransformer.transform(inputJson, mappingJson);
        assertJsonEquals(expectedOutput, actualOutput);
    }

    /**
     * Helper method to compare JSON strings for structural equality using Jackson.
     */
    private static void assertJsonEquals(String expected, String actual) throws JsonProcessingException {
        JsonNode expectedNode = MAPPER.readTree(expected);
        JsonNode actualNode = MAPPER.readTree(actual);
        assertEquals("JSON structures do not match", expectedNode, actualNode);
    }
}
