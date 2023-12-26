/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class WorkflowNodeTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testNode() throws IOException {

        WorkflowNode nodeA = new WorkflowNode(
            "A",
            "a-type",
            Map.of("foo", "field"),
            Map.ofEntries(
                Map.entry("foo", "a string"),
                Map.entry("bar", Map.of("key", "value")),
                Map.entry("baz", new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") }),
                Map.entry("qux", false),
                Map.entry("processors", new PipelineProcessor[] { new PipelineProcessor("test-type", Map.of("key2", "value2")) }),
                Map.entry("created_time", 1689793598499L),
                Map.entry("tools_order", new String[] { "foo", "bar" })
            )
        );
        assertEquals("A", nodeA.id());
        assertEquals("a-type", nodeA.type());
        assertEquals(Map.of("foo", "field"), nodeA.previousNodeInputs());
        Map<String, Object> map = nodeA.userInputs();
        assertEquals("a string", (String) map.get("foo"));
        assertEquals(Map.of("key", "value"), (Map<?, ?>) map.get("bar"));
        assertArrayEquals(new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") }, (Map<?, ?>[]) map.get("baz"));
        assertFalse((Boolean) map.get("qux"));
        PipelineProcessor[] pp = (PipelineProcessor[]) map.get("processors");
        assertEquals(1, pp.length);
        assertEquals("test-type", pp[0].type());
        assertEquals(Map.of("key2", "value2"), pp[0].params());
        assertEquals(1689793598499L, map.get("created_time"));
        assertArrayEquals(new String[] { "foo", "bar" }, (String[]) map.get("tools_order"));

        // node equality is based only on ID
        WorkflowNode nodeA2 = new WorkflowNode("A", "a2-type", Map.of(), Map.of("bar", "baz"));
        assertEquals(nodeA, nodeA2);

        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("A", "foo"), Map.of("baz", "qux"));
        assertNotEquals(nodeA, nodeB);

        String json = TemplateTestJsonUtil.parseToJson(nodeA);
        assertTrue(json.startsWith("{\"id\":\"A\",\"type\":\"a-type\",\"previous_node_inputs\":{\"foo\":\"field\"},"));
        assertTrue(json.contains("\"user_inputs\":{"));
        assertTrue(json.contains("\"foo\":\"a string\""));
        assertTrue(json.contains("\"baz\":[{\"A\":\"a\"},{\"B\":\"b\"}]"));
        assertTrue(json.contains("\"bar\":{\"key\":\"value\"}"));
        assertTrue(json.contains("\"qux\":false"));
        assertTrue(json.contains("\"processors\":[{\"type\":\"test-type\",\"params\":{\"key2\":\"value2\"}}]"));
        assertTrue(json.contains("\"created_time\":1689793598499"));
        assertTrue(json.contains("\"tools_order\":[\"foo\",\"bar\"]"));

        WorkflowNode nodeX = WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals("A", nodeX.id());
        assertEquals("a-type", nodeX.type());
        Map<String, String> previousNodeInputs = nodeX.previousNodeInputs();
        assertEquals("field", previousNodeInputs.get("foo"));
        Map<String, Object> mapX = nodeX.userInputs();
        assertEquals("a string", mapX.get("foo"));
        assertEquals(Map.of("key", "value"), mapX.get("bar"));
        assertArrayEquals(new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") }, (Map<?, ?>[]) map.get("baz"));
        PipelineProcessor[] ppX = (PipelineProcessor[]) map.get("processors");
        assertEquals(1, ppX.length);
        assertEquals("test-type", ppX[0].type());
        assertEquals(Map.of("key2", "value2"), ppX[0].params());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"badField\":\"A\",\"type\":\"a-type\",\"user_inputs\":{\"foo\":\"bar\"}}";
        IOException e = assertThrows(IOException.class, () -> WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(badJson)));
        assertEquals("Unable to parse field [badField] in a node object.", e.getMessage());

        String missingJson = "{\"id\":\"A\",\"user_inputs\":{\"foo\":\"bar\"}}";
        e = assertThrows(IOException.class, () -> WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(missingJson)));
        assertEquals("An node object requires both an id and type field.", e.getMessage());
    }
}
