/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class WorkflowDataTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testWorkflowData() {

        WorkflowData empty = WorkflowData.EMPTY;
        assertTrue(empty.getParams().isEmpty());
        assertTrue(empty.getContent().isEmpty());

        Map<String, Object> expectedContent = Map.of("baz", new String[] { "qux", "quxx" });
        WorkflowData contentOnly = new WorkflowData(expectedContent, "test-id-123");
        assertTrue(contentOnly.getParams().isEmpty());
        assertEquals(expectedContent, contentOnly.getContent());

        Map<String, String> expectedParams = Map.of("foo", "bar");
        WorkflowData contentAndParams = new WorkflowData(expectedContent, expectedParams, "test-id-123");
        assertEquals(expectedParams, contentAndParams.getParams());
        assertEquals(expectedContent, contentAndParams.getContent());
        assertEquals("test-id-123", contentAndParams.getWorkflowId());
    }
}
