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
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for JsonToJsonRecommender methods.
 * Tests the main entry points for getting JSON transformation recommendations,
 * including both string-based and JsonNode-based APIs with various validation options.
 */
public class JsonToJsonRecommenderTests extends OpenSearchTestCase {
    /**
     * ObjectMapper instance used for parsing JSON strings and formatting output in tests.
     * Configured with relaxed constraints to allow testing edge cases.
     */
    private static final ObjectMapper MAPPER;
    static {
        StreamReadConstraints constraints = StreamReadConstraints.builder()
            .maxNestingDepth(CommonValue.MAX_JSON_NESTING_DEPTH + 10)
            .maxStringLength(CommonValue.MAX_JSON_SIZE)
            .maxNameLength(CommonValue.MAX_JSON_NAME_LENGTH + 10)
            .build();

        MAPPER = new ObjectMapper();
        MAPPER.getFactory().setStreamReadConstraints(constraints);
    }

    public void testSimpleNestedStructures() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
              "level1": {
                "level2": {
                  "level3": {
                    "data": "deep_value"
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "result": {
                "nested": {
                  "info": {
                    "value": "deep_value"
                  }
                }
              }
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains the expected structure (ignoring
        // whitespace differences)
        assertTrue(
            "Detailed JSON should contain expected mapping",
            output.detailedJsonPathString.contains("\"value\" : \"$.level1.level2.level3.data\"")
        );
        assertTrue(
            "Generalized JSON should contain expected mapping",
            output.generalizedJsonPathString.contains("\"value\" : \"$.level1.level2.level3.data\"")
        );
    }

    public void testArrayToArrayWithGeneralized() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {"numbers": [1, 2, 3, 4, 5]}
            """;

        String outputJson = """
            {"values": [1, 2, 3, 4, 5]}
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        /*
         * Expected detailed output should contain:
         * {
         * "values[0]" : "$.numbers[0]",
         * "values[1]" : "$.numbers[1]",
         * "values[2]" : "$.numbers[2]",
         * "values[3]" : "$.numbers[3]",
         * "values[4]" : "$.numbers[4]"
         * }
         */

        /*
         * Expected generalized output should contain:
         * {
         * "values[*]" : "$.numbers[*]"
         * }
         */

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue(
            "Detailed JSON should contain individual array mappings",
            output.detailedJsonPathString.contains("\"values[0]\" : \"$.numbers[0]\"")
        );
        assertTrue(
            "Detailed JSON should contain all array elements",
            output.detailedJsonPathString.contains("\"values[4]\" : \"$.numbers[4]\"")
        );
        assertTrue(
            "Generalized JSON should contain wildcard mapping",
            output.generalizedJsonPathString.contains("\"values[*]\" : \"$.numbers[*]\"")
        );
    }

    public void testArrayToArrayCannotBeGeneralized() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {"numbers": [1, 2, 3, 4, 5]}
            """;

        String outputJson = """
            {"values": [1, 2, 3, 4, 6]}
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue(
            "Detailed JSON should contain individual array mappings",
            output.detailedJsonPathString.contains("\"values[0]\" : \"$.numbers[0]\"")
        );
        assertTrue(
            "Detailed JSON should contain mapped elements",
            output.detailedJsonPathString.contains("\"values[3]\" : \"$.numbers[3]\"")
        );
        assertTrue("Detailed JSON should not contain unmapped elements", !output.detailedJsonPathString.contains("\"values[4]\""));

        // Since arrays cannot be generalized (different values), both outputs should be
        // the same
        assertTrue(
            "Generalized JSON should also contain individual mappings",
            output.generalizedJsonPathString.contains("\"values[0]\" : \"$.numbers[0]\"")
        );
        assertTrue("Generalized JSON should not contain wildcard patterns", !output.generalizedJsonPathString.contains("\"values[*]\""));
    }

    public void testOneToManyMapping() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
                "user": {
                    "id": "u123",
                    "skills": ["Java", "Python", "Go"]
                }
            }
            """;

        String outputJson = """
            {
                "skills" : [
                    {
                        "user_id": "u123",
                        "skill": "Java"
                    },
                    {
                        "user_id": "u123",
                        "skill": "Python"
                    },
                    {
                        "user_id": "u123",
                        "skill": "Go"
                    }
                ]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue("Detailed JSON should contain user_id mapping", output.detailedJsonPathString.contains("\"user_id\" : \"$.user.id\""));
        assertTrue(
            "Detailed JSON should contain skills array mappings",
            output.detailedJsonPathString.contains("\"skill\" : \"$.user.skills[0]\"")
        );
        assertTrue("Detailed JSON should contain multiple skill elements", output.detailedJsonPathString.contains("\"skills[2]\""));

        assertTrue(
            "Generalized JSON should contain wildcard user_id mapping",
            output.generalizedJsonPathString.contains("\"user_id\" : \"$.user.id\"")
        );
        assertTrue(
            "Generalized JSON should contain wildcard skills mapping",
            output.generalizedJsonPathString.contains("\"skill\" : \"$.user.skills[*]\"")
        );
        assertTrue("Generalized JSON should use wildcard pattern", output.generalizedJsonPathString.contains("\"skills[*]\""));
    }

    public void testManyToOneMapping() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            [
                {
                    "hits": [
                        {
                            "_source": {
                                "books": { "name": "To Kill a Mockingbird" },
                                "songs": { "name": "Pocketful of Sunshine" }
                            }
                        }
                    ]
                },
                {
                    "hits": [
                        {
                            "_source": {
                                "books": { "name": "Where the Crawdads Sing" },
                                "songs": { "name": "If" }
                            }
                        }
                    ]
                }
            ]
            """;

        String outputJson = """
            {
                "book_name": ["To Kill a Mockingbird", "Where the Crawdads Sing"],
                "song_name": ["Pocketful of Sunshine", "If"]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue(
            "Detailed JSON should contain book mappings",
            output.detailedJsonPathString.contains("\"book_name[0]\" : \"$[0].hits[0]._source.books.name\"")
        );
        assertTrue(
            "Detailed JSON should contain song mappings",
            output.detailedJsonPathString.contains("\"song_name[0]\" : \"$[0].hits[0]._source.songs.name\"")
        );
        assertTrue("Detailed JSON should contain multiple array elements", output.detailedJsonPathString.contains("\"book_name[1]\""));

        assertTrue(
            "Generalized JSON should contain wildcard book mapping",
            output.generalizedJsonPathString.contains("\"book_name[*]\" : \"$[*].hits[0]._source.books.name\"")
        );
        assertTrue(
            "Generalized JSON should contain wildcard song mapping",
            output.generalizedJsonPathString.contains("\"song_name[*]\" : \"$[*].hits[0]._source.songs.name\"")
        );
    }

    public void testComplexArrayToArray() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": {
                  "useful": {
                    "updatedAt": "2020-08-10T14:26:48-07:00",
                    "token": 1134,
                    "items": [
                      {
                        "allocation": 0.2,
                        "team": {
                          "id": 90,
                          "name": "Some Team Name 1",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.9,
                        "team": {
                          "id": 80,
                          "name": "Some Team Name 2",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.1,
                        "team": {
                          "id": 10,
                          "name": "Some Team Name 3",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "allocDetails": [
                {
                  "allocation": 0.2,
                  "team": {
                    "id": 90,
                    "name": "Some Team Name 1"
                  }
                },
                {
                  "allocation": 0.9,
                  "team": {
                    "id": 80,
                    "name": "Some Team Name 2"
                  }
                },
                {
                  "allocation": 0.1,
                  "team": {
                    "id": 10,
                    "name": "Some Team Name 3"
                  }
                }
              ]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue("Detailed JSON should contain name mapping", output.detailedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Detailed JSON should contain allocation mappings",
            output.detailedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails.useful.items[0].allocation\"")
        );
        assertTrue(
            "Detailed JSON should contain team id mappings",
            output.detailedJsonPathString.contains("\"id\" : \"$.item.allocDetails.useful.items[0].team.id\"")
        );
        assertTrue(
            "Detailed JSON should contain team name mappings",
            output.detailedJsonPathString.contains("\"name\" : \"$.item.allocDetails.useful.items[0].team.name\"")
        );

        assertTrue("Generalized JSON should contain name mapping", output.generalizedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Generalized JSON should contain wildcard allocation mappings",
            output.generalizedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails.useful.items[*].allocation\"")
        );
        assertTrue(
            "Generalized JSON should use wildcard pattern for arrays",
            output.generalizedJsonPathString.contains("\"allocDetails[*]\"")
        );
    }

    public void testComplexArrayToArrayWithPartialInputGeneralization() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": [
                  {
                    "useful": {
                      "updatedAt": "2020-08-10T14:26:48-07:00",
                      "token": 1134,
                      "items": [
                        {
                          "allocation": 0.2,
                          "team": {
                            "id": 90,
                            "name": "Some Team Name 1",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.9,
                          "team": {
                            "id": 80,
                            "name": "Some Team Name 2",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.1,
                          "team": {
                            "id": 10,
                            "name": "Some Team Name 3",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        }
                      ]
                    }
                  },
                  {
                    "useless": {
                      "aa": "bb"
                    }
                  }
                ]
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "allocDetails": [
                {
                  "allocation": 0.2,
                  "team": {
                    "id": 90,
                    "name": "Some Team Name 1"
                  }
                },
                {
                  "allocation": 0.9,
                  "team": {
                    "id": 80,
                    "name": "Some Team Name 2"
                  }
                },
                {
                  "allocation": 0.1,
                  "team": {
                    "id": 10,
                    "name": "Some Team Name 3"
                  }
                }
              ]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue("Detailed JSON should contain name mapping", output.detailedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Detailed JSON should contain allocation mapping with array index",
            output.detailedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails[0].useful.items[0].allocation\"")
        );
        assertTrue(
            "Detailed JSON should contain team mappings with array indices",
            output.detailedJsonPathString.contains("\"id\" : \"$.item.allocDetails[0].useful.items[0].team.id\"")
        );

        assertTrue("Generalized JSON should contain name mapping", output.generalizedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Generalized JSON should contain partial wildcard mapping",
            output.generalizedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails[0].useful.items[*].allocation\"")
        );
        assertTrue(
            "Generalized JSON should use wildcard for output arrays",
            output.generalizedJsonPathString.contains("\"allocDetails[*]\"")
        );
    }

    public void testComplexArrayToArrayWithPartialOutputGeneralization() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": {
                  "useful": {
                    "updatedAt": "2020-08-10T14:26:48-07:00",
                    "token": 1134,
                    "items": [
                      {
                        "allocation": 0.2,
                        "team": {
                          "id": 90,
                          "name": "Some Team Name 1",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.9,
                        "team": {
                          "id": 80,
                          "name": "Some Team Name 2",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.1,
                        "team": {
                          "id": 10,
                          "name": "Some Team Name 3",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "temp": [
                {
                  "allocDetails": [
                    {
                      "allocation": 0.2,
                      "team": {
                        "id": 90,
                        "name": "Some Team Name 1"
                      }
                    },
                    {
                      "allocation": 0.9,
                      "team": {
                        "id": 80,
                        "name": "Some Team Name 2"
                      }
                    },
                    {
                      "allocation": 0.1,
                      "team": {
                        "id": 10,
                        "name": "Some Team Name 3"
                      }
                    }
                  ]
                },
                {
                  "unuseful": "haha"
                }
              ]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue("Detailed JSON should contain name mapping", output.detailedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue("Detailed JSON should contain temp[0] structure", output.detailedJsonPathString.contains("\"temp[0]\""));
        assertTrue(
            "Detailed JSON should contain allocation mappings",
            output.detailedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails.useful.items[0].allocation\"")
        );
        assertTrue(
            "Detailed JSON should contain team mappings",
            output.detailedJsonPathString.contains("\"id\" : \"$.item.allocDetails.useful.items[0].team.id\"")
        );

        // Verify generalized JSON contains specific temp array elements
        assertTrue("Generalized JSON should contain name mapping", output.generalizedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Generalized JSON should contain temp[0] with wildcard allocDetails",
            output.generalizedJsonPathString.contains("\"temp[0]\"") && output.generalizedJsonPathString.contains("\"allocDetails[*]\"")
        );
        assertTrue(
            "Generalized JSON should contain wildcard allocation mappings in temp[0]",
            output.generalizedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails.useful.items[*].allocation\"")
        );
    }

    public void testComplexArrayToArrayWithPartialOutputAndInputGeneralization() throws JsonProcessingException, IllegalArgumentException {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": [
                  {
                    "useful": {
                      "updatedAt": "2020-08-10T14:26:48-07:00",
                      "token": 1134,
                      "items": [
                        {
                          "allocation": 0.2,
                          "team": {
                            "id": 90,
                            "name": "Some Team Name 1",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.9,
                          "team": {
                            "id": 80,
                            "name": "Some Team Name 2",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.1,
                          "team": {
                            "id": 10,
                            "name": "Some Team Name 3",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        }
                      ]
                    }
                  },
                  {
                    "useless": {
                      "aa": "bb"
                    }
                  }
                ]
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "temp": [
                {
                  "allocDetails": [
                    {
                      "allocation": 0.2,
                      "team": {
                        "id": 90,
                        "name": "Some Team Name 1"
                      }
                    },
                    {
                      "allocation": 0.9,
                      "team": {
                        "id": 80,
                        "name": "Some Team Name 2"
                      }
                    },
                    {
                      "allocation": 0.1,
                      "team": {
                        "id": 10,
                        "name": "Some Team Name 3"
                      }
                    }
                  ]
                },
                {
                  "allocDetails2": {
                    "allocation": 0.2,
                    "team": {
                      "id": 90,
                      "name": "Some Team Name 1"
                    }
                  }
                },
                {
                  "unuseful": "haha"
                }
              ]
            }
            """;

        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);

        // Verify that the actual output contains expected mappings (ignoring formatting
        // differences)
        assertTrue("Detailed JSON should contain name mapping", output.detailedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue("Detailed JSON should contain temp[0] structure", output.detailedJsonPathString.contains("\"temp[0]\""));
        assertTrue("Detailed JSON should contain temp[1] structure", output.detailedJsonPathString.contains("\"temp[1]\""));
        assertTrue(
            "Detailed JSON should contain allocation mappings",
            output.detailedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails[0].useful.items[0].allocation\"")
        );
        assertTrue("Detailed JSON should contain allocDetails2 mapping", output.detailedJsonPathString.contains("\"allocDetails2\""));

        // Verify generalized JSON contains specific temp array elements
        assertTrue("Generalized JSON should contain name mapping", output.generalizedJsonPathString.contains("\"name\" : \"$.item.name\""));
        assertTrue(
            "Generalized JSON should contain temp[0] with wildcard allocDetails",
            output.generalizedJsonPathString.contains("\"temp[0]\"") && output.generalizedJsonPathString.contains("\"allocDetails[*]\"")
        );
        assertTrue(
            "Generalized JSON should contain temp[1] with allocDetails2",
            output.generalizedJsonPathString.contains("\"temp[1]\"") && output.generalizedJsonPathString.contains("\"allocDetails2\"")
        );
        assertTrue(
            "Generalized JSON should contain partial wildcard mapping for temp[0]",
            output.generalizedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails[0].useful.items[*].allocation\"")
        );
        assertTrue(
            "Generalized JSON should contain specific mapping for temp[1]",
            output.generalizedJsonPathString.contains("\"allocation\" : \"$.item.allocDetails[0].useful.items[0].allocation\"")
        );
    }

    public void testGetRecommendationErrorHandlingForNullInputs() {
        // Test null inputs
        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat(null, "{}"));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("{}", null));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInMapFormat(null, MAPPER.readTree("{}")));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInMapFormat(MAPPER.readTree("{}"), null));

    }

    public void testGetRecommendationErrorHandlingForEmptyInputs() {
        // Test empty inputs
        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("", "{}"));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("{}", ""));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("   ", "{}"));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("{}", "   "));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat("{}", "{}"));
    }

    public void testGetRecommendationWithInvalidJson() {
        String invalidJson = "{invalid json}";
        String validJson = "{\"test_key\": \"test_value\"}";

        // Should throw JsonProcessingException for invalid input JSON
        expectThrows(JsonProcessingException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat(invalidJson, validJson));

        // Should throw JsonProcessingException for invalid output JSON
        expectThrows(JsonProcessingException.class, () -> JsonToJsonRecommender.getRecommendationInStringFormat(validJson, invalidJson));
    }

    public void testGetRecommendationWithOversizedJson() {
        // Create a large JSON string that exceeds the StreamReadConstraints limit
        // (50MB)
        StringBuilder largeJsonBuilder = new StringBuilder("{\"data\": \"");
        // Create a string larger than 50MB (50,000,000 characters)
        for (int i = 0; i < 50_043_091; i++) {
            largeJsonBuilder.append("a");
        }
        largeJsonBuilder.append("\"}");
        String largeJson = largeJsonBuilder.toString();
        String validJson = "{\"result\": \"test_value\"}";

        // Should throw JsonProcessingException for oversized input JSON
        JsonProcessingException inputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(largeJson, validJson)
        );
        assertTrue(
            "Exception message should mention string value length exceeds maximum",
            inputException.getMessage().contains("String value length")
                && inputException.getMessage().contains("exceeds the maximum allowed")
                && inputException.getMessage().contains("StreamReadConstraints.getMaxStringLength()")
        );

        // Should throw JsonProcessingException for oversized output JSON
        JsonProcessingException outputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(validJson, largeJson)
        );
        assertTrue(
            "Exception message should mention string value length exceeds maximum",
            outputException.getMessage().contains("String value length")
                && outputException.getMessage().contains("exceeds the maximum allowed")
                && outputException.getMessage().contains("StreamReadConstraints.getMaxStringLength()")
        );
    }

    public void testGetRecommendationWithOversizedFieldName() throws JsonProcessingException {
        // Create a JSON string with field name that exceeds the MAX_NAME_LEN limit
        // (50000 characters)
        StringBuilder longFieldNameBuilder = new StringBuilder();
        // Create a field name longer than 50000 characters
        for (int i = 0; i < 50001; i++) {
            longFieldNameBuilder.append("a");
        }
        String longFieldName = longFieldNameBuilder.toString();
        String inputJsonWithLongFieldName = "{\"" + longFieldName + "\": \"test_value\"}";
        String validJson = "{\"result\": \"test_value\"}";

        // Should throw JsonProcessingException for oversized field name in input JSON
        JsonProcessingException inputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(inputJsonWithLongFieldName, validJson)
        );
        assertTrue(
            "Exception message should mention field name length exceeds maximum",
            inputException.getMessage().contains("Name length")
                && inputException.getMessage().contains("exceeds the maximum allowed")
                && inputException.getMessage().contains("StreamReadConstraints.getMaxNameLength()")
        );

        // Should throw JsonProcessingException for oversized field name in output JSON
        JsonProcessingException outputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(validJson, inputJsonWithLongFieldName)
        );
        assertTrue(
            "Exception message should mention field name length exceeds maximum",
            outputException.getMessage().contains("Name length")
                && outputException.getMessage().contains("exceeds the maximum allowed")
                && outputException.getMessage().contains("StreamReadConstraints.getMaxNameLength()")
        );

        // Add test for oversized field name in JsonNode format in input Node
        JsonNode inputNode = MAPPER.readTree(inputJsonWithLongFieldName);
        JsonNode outputNode = MAPPER.readTree(validJson);
        IllegalArgumentException inputNodeException = expectThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonRecommender.getRecommendationInMapFormat(inputNode, outputNode)
        );
        assertTrue(
            "Exception message should mention field name length exceeds maximum",
            inputNodeException.getMessage().contains("Input JSON Node")
                && inputNodeException.getMessage().contains("contains a key with length")
                && inputNodeException.getMessage().contains("exceeds the maximum allowed")
        );

        // Add test for oversized field name in JsonNode format in output Node
        IllegalArgumentException outputNodeException = expectThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonRecommender.getRecommendationInMapFormat(outputNode, inputNode)
        );
        assertTrue(
            "Exception message should mention field name length exceeds maximum",
            outputNodeException.getMessage().contains("Output JSON Node")
                && outputNodeException.getMessage().contains("contains a key with length")
                && outputNodeException.getMessage().contains("exceeds the maximum allowed")
        );
    }

    public void testGetRecommendationWithDeeplyNestedJson() throws JsonProcessingException {
        // Create a deeply nested JSON that exceeds the nesting limit (1000 levels)
        StringBuilder deepJsonBuilder = new StringBuilder();
        for (int i = 0; i < 1001; i++) { // Exceed the limit of 1000
            deepJsonBuilder.append("{\"level").append(i).append("\":");
        }
        deepJsonBuilder.append("\"deep_value\"");
        for (int i = 0; i < 1001; i++) {
            deepJsonBuilder.append("}");
        }
        String deepJson = deepJsonBuilder.toString();
        String validJson = "{\"result\": \"deep_value\"}";

        // Should throw JsonProcessingException for deeply nested input JSON
        JsonProcessingException inputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(deepJson, validJson)
        );
        assertTrue(
            inputException.getMessage(),
            inputException.getMessage().contains("Document nesting depth")
                && inputException.getMessage().contains("exceeds the maximum allowed")
                && inputException.getMessage().contains("StreamReadConstraints.getMaxNestingDepth()")
        );

        // Should throw JsonProcessingException for deeply nested output JSON
        JsonProcessingException outputException = expectThrows(
            JsonProcessingException.class,
            () -> JsonToJsonRecommender.getRecommendationInStringFormat(validJson, deepJson)
        );
        assertTrue(
            "Exception message should mention document nesting depth exceeds maximum",
            outputException.getMessage().contains("Document nesting depth")
                && outputException.getMessage().contains("exceeds the maximum allowed")
                && outputException.getMessage().contains("StreamReadConstraints.getMaxNestingDepth()")
        );

        // Add test for deeply nested JsonNode format in input Node
        JsonNode inputNode = MAPPER.readTree(deepJson);
        JsonNode outputNode = MAPPER.readTree(validJson);
        IllegalArgumentException inputNodeException = expectThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonRecommender.getRecommendationInMapFormat(inputNode, outputNode)
        );
        assertTrue(
            "Exception message should mention document nesting depth exceeds maximum",
            inputNodeException.getMessage().contains("Input JSON Node")
                && inputNodeException.getMessage().contains("nesting depth")
                && inputNodeException.getMessage().contains("exceeds the maximum allowed")
        );

        // Add test for deeply nested JsonNode format in output Node
        IllegalArgumentException outputNodeException = expectThrows(
            IllegalArgumentException.class,
            () -> JsonToJsonRecommender.getRecommendationInMapFormat(outputNode, inputNode)
        );
        assertTrue(
            "Exception message should mention document nesting depth exceeds maximum",
            outputNodeException.getMessage().contains("Output JSON Node")
                && outputNodeException.getMessage().contains("nesting depth")
                && outputNodeException.getMessage().contains("exceeds the maximum allowed")
        );
    }

    public void testGetRecommendationWithValidSizeAndNesting() throws JsonProcessingException, IllegalArgumentException {
        // Test JSON that's within both size and nesting limits
        StringBuilder validNestedJson = new StringBuilder();
        for (int i = 0; i < 999; i++) { // Well within the limit of 1000
            validNestedJson.append("{\"level").append(i).append("\":");
        }
        validNestedJson.append("\"test_value\"");
        for (int i = 0; i < 999; i++) {
            validNestedJson.append("}");
        }

        String inputJson = validNestedJson.toString();
        String outputJson = "{\"result\": \"test_value\"}";

        // Should not throw any exception
        JsonToJsonRecommender.StringFormatResult output = JsonToJsonRecommender.getRecommendationInStringFormat(inputJson, outputJson);
        assertNotNull("Should have detailed mapping", output.detailedJsonPathString);
        assertNotNull("Should have generalized mapping", output.generalizedJsonPathString);
    }
}
