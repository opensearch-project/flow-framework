/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class WorkflowNodeTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testNode() throws IOException {
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        assertEquals("A", nodeA.id());
        assertEquals("a-type", nodeA.type());
        assertEquals(Map.of("foo", "bar"), nodeA.inputs());

        // node equality is based only on ID
        WorkflowNode nodeA2 = new WorkflowNode("A", "a2-type", Map.of("bar", "baz"));
        assertEquals(nodeA, nodeA2);

        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        assertNotEquals(nodeA, nodeB);

        String expectedJson = "{\"id\":\"A\",\"type\":\"a-type\",\"inputs\":{\"foo\":\"bar\"}}";
        String json = TemplateTestJsonUtil.parseToJson(nodeA);
        assertEquals(expectedJson, json);

        XContentParser parser = TemplateTestJsonUtil.jsonToParser(json);
        WorkflowNode nodeX = WorkflowNode.parse(parser);
        assertEquals("A", nodeX.id());
        assertEquals("a-type", nodeX.type());
        assertEquals(Map.of("foo", "bar"), nodeX.inputs());
    }
}
