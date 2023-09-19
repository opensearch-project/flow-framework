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

import java.util.Collections;

public class WorkflowDataTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testWorkflowData() {
        WorkflowData data = new WorkflowData() {
        };
        assertEquals(Collections.emptyMap(), data.getParams());
        assertEquals(Collections.emptyMap(), data.getContent());
    }
}
