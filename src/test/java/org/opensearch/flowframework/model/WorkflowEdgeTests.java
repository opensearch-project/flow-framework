/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class WorkflowEdgeTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testEdge() throws IOException {
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        assertEquals("A", edgeAB.source());
        assertEquals("B", edgeAB.destination());
        assertEquals("A->B", edgeAB.toString());

        WorkflowEdge edgeAB2 = new WorkflowEdge("A", "B");
        assertEquals(edgeAB, edgeAB2);

        WorkflowEdge edgeAC = new WorkflowEdge("A", "C");
        assertNotEquals(edgeAB, edgeAC);

        String expectedJson = "{\"source\":\"A\",\"dest\":\"B\"}";
        String json = TemplateTestJsonUtil.parseToJson(edgeAB);
        assertEquals(expectedJson, json);

        WorkflowEdge edgeX = WorkflowEdge.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals("A", edgeX.source());
        assertEquals("B", edgeX.destination());
        assertEquals("A->B", edgeX.toString());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"badField\":\"A\",\"dest\":\"B\"}";
        FlowFrameworkException e = assertThrows(
            FlowFrameworkException.class,
            () -> WorkflowEdge.parse(TemplateTestJsonUtil.jsonToParser(badJson))
        );
        assertEquals("Unable to parse field [badField] in an edge object.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());

        String missingJson = "{\"dest\":\"B\"}";
        e = assertThrows(FlowFrameworkException.class, () -> WorkflowEdge.parse(TemplateTestJsonUtil.jsonToParser(missingJson)));
        assertEquals("An edge object requires both a source and dest field.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());
    }

}
