/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

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
            Map.ofEntries(
                Map.entry("foo", "a string"),
                Map.entry("bar", Map.of("key", "value")),
                Map.entry("baz", new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") })
            )
        );
        assertEquals("A", nodeA.id());
        assertEquals("a-type", nodeA.type());
        Map<String, Object> map = nodeA.inputs();
        assertEquals("a string", (String) map.get("foo"));
        assertEquals(Map.of("key", "value"), (Map<?, ?>) map.get("bar"));
        assertArrayEquals(new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") }, (Map<?, ?>[]) map.get("baz"));

        // node equality is based only on ID
        WorkflowNode nodeA2 = new WorkflowNode("A", "a2-type", Map.of("bar", "baz"));
        assertEquals(nodeA, nodeA2);

        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        assertNotEquals(nodeA, nodeB);

        String json = TemplateTestJsonUtil.parseToJson(nodeA);
        assertTrue(json.startsWith("{\"id\":\"A\",\"type\":\"a-type\",\"inputs\":"));
        assertTrue(json.contains("\"foo\":\"a string\""));
        assertTrue(json.contains("\"baz\":[{\"A\":\"a\"},{\"B\":\"b\"}]"));
        assertTrue(json.contains("\"bar\":{\"key\":\"value\"}"));

        WorkflowNode nodeX = WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals("A", nodeX.id());
        assertEquals("a-type", nodeX.type());
        Map<String, Object> mapX = nodeX.inputs();
        assertEquals("a string", mapX.get("foo"));
        assertEquals(Map.of("key", "value"), mapX.get("bar"));
        assertArrayEquals(new Map<?, ?>[] { Map.of("A", "a"), Map.of("B", "b") }, (Map<?, ?>[]) map.get("baz"));
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"badField\":\"A\",\"type\":\"a-type\",\"inputs\":{\"foo\":\"bar\"}}";
        IOException e = assertThrows(IOException.class, () -> WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(badJson)));
        assertEquals("Unable to parse field [badField] in a node object.", e.getMessage());

        String missingJson = "{\"id\":\"A\",\"inputs\":{\"foo\":\"bar\"}}";
        e = assertThrows(IOException.class, () -> WorkflowNode.parse(TemplateTestJsonUtil.jsonToParser(missingJson)));
        assertEquals("An node object requires both an id and type field.", e.getMessage());
    }
}
