/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParseUtilsTests extends OpenSearchTestCase {
    public void testResourceToStringToJson() throws IOException {
        String json = ParseUtils.resourceToString("/template/finaltemplate.json");
        assertTrue(json.startsWith("{"));
        assertTrue(json.contains("name"));
        try (XContentParser parser = ParseUtils.jsonToParser(json)) {
            assertEquals(Token.FIELD_NAME, parser.nextToken());
            assertEquals("name", parser.currentName());
        }
    }

    public void testToInstant() throws IOException {
        long epochMilli = Instant.now().toEpochMilli();
        XContentBuilder builder = XContentFactory.jsonBuilder().value(epochMilli);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.parseInstant(parser);
        assertEquals(epochMilli, instant.toEpochMilli());
    }

    public void testToInstantWithNullToken() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value((Long) null);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertEquals(token, XContentParser.Token.VALUE_NULL);
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNullValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().value(randomLong());
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        parser.nextToken();
        XContentParser.Token token = parser.currentToken();
        assertNull(token);
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testToInstantWithNotValue() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().nullField("test").endObject();
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Instant instant = ParseUtils.parseInstant(parser);
        assertNull(instant);
    }

    public void testBuildAndParseStringToStringMap() throws IOException {
        Map<String, String> stringMap = Map.ofEntries(Map.entry("one", "two"));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ParseUtils.buildStringToStringMap(builder, stringMap);
        XContentParser parser = this.createParser(builder);
        parser.nextToken();
        Map<String, String> parsedMap = ParseUtils.parseStringToStringMap(parser);
        assertEquals(stringMap.get("one"), parsedMap.get("one"));
    }

    public void testGetInputsFromPreviousSteps() {
        WorkflowData currentNodeInputs = new WorkflowData(
            Map.ofEntries(
                Map.entry("content1", 1),
                Map.entry("param1", 2),
                Map.entry("content3", "${{step1.output1}}"),
                Map.entry("nestedMap", Map.of("content4", "${{step3.output3}}")),
                Map.entry("nestedList", List.of("${{step4.output4}}"))
            ),
            Map.of("param1", "value1"),
            "workflowId",
            "nodeId"
        );
        Map<String, WorkflowData> outputs = Map.ofEntries(
            Map.entry(
                "step1",
                new WorkflowData(
                    Map.ofEntries(Map.entry("output1", "outputvalue1"), Map.entry("output2", "step1outputvalue2")),
                    "workflowId",
                    "step1"
                )
            ),
            Map.entry("step2", new WorkflowData(Map.of("output2", "step2outputvalue2"), "workflowId", "step2")),
            Map.entry("step3", new WorkflowData(Map.of("output3", "step3outputvalue3"), "workflowId", "step3")),
            Map.entry("step4", new WorkflowData(Map.of("output4", "step4outputvalue4"), "workflowId", "step4"))
        );
        Map<String, String> previousNodeInputs = Map.of("step2", "output2");
        Set<String> requiredKeys = Set.of("param1", "content1");
        Set<String> optionalKeys = Set.of("output1", "output2", "content3", "nestedMap", "nestedList", "no-output");
        Map<String, String> params = Map.of("param1", "value1");
        Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
            requiredKeys,
            optionalKeys,
            currentNodeInputs,
            outputs,
            previousNodeInputs,
            params
        );

        assertEquals("value1", inputs.get("param1"));
        assertEquals(1, inputs.get("content1"));
        assertEquals("outputvalue1", inputs.get("output1"));
        assertEquals("step2outputvalue2", inputs.get("output2"));

        // FIXME add a substitution test for params here
        // Substitutions
        assertEquals("outputvalue1", inputs.get("content3"));
        @SuppressWarnings("unchecked")
        Map<String, Object> nestedMap = (Map<String, Object>) inputs.get("nestedMap");
        assertEquals("step3outputvalue3", nestedMap.get("content4"));
        @SuppressWarnings("unchecked")
        List<String> nestedList = (List<String>) inputs.get("nestedList");
        assertEquals(List.of("step4outputvalue4"), nestedList);
        assertNull(inputs.get("no-output"));

        Set<String> missingRequiredKeys = Set.of("not-here");
        FlowFrameworkException e = assertThrows(
            FlowFrameworkException.class,
            () -> ParseUtils.getInputsFromPreviousSteps(
                missingRequiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            )
        );
        assertEquals("Missing required inputs [not-here] in workflow [workflowId] node [nodeId]", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());
    }
}
