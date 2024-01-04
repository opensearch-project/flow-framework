/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class WorkflowStateTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testWorkflowState() throws IOException {
        String workflowId = "id";
        String error = "error";
        String state = "state";
        String provisioningProgress = "progress";
        Instant provisionStartTime = Instant.now().minusSeconds(2);
        Instant provisionEndTime = Instant.now();
        Map<String, Object> userOutputs = Map.of("foo", Map.of("bar", "baz"));
        List<ResourceCreated> resourcesCreated = List.of(new ResourceCreated("name", "stepId", "type", "id"));

        WorkflowState wfs = WorkflowState.builder()
            .workflowId(workflowId)
            .error(error)
            .state(state)
            .provisioningProgress(provisioningProgress)
            .provisionStartTime(provisionStartTime)
            .provisionEndTime(provisionEndTime)
            // TODO test this
            // .user(user)
            .userOutputs(userOutputs)
            .resourcesCreated(resourcesCreated)
            .build();

        assertEquals(workflowId, wfs.getWorkflowId());
        assertEquals(error, wfs.getError());
        assertEquals(state, wfs.getState());
        assertEquals(provisioningProgress, wfs.getProvisioningProgress());
        assertEquals(provisionStartTime, wfs.getProvisionStartTime());
        assertEquals(provisionEndTime, wfs.getProvisionEndTime());
        assertEquals(1, wfs.userOutputs().size());
        assertEquals("baz", ((Map<?, ?>) wfs.userOutputs().get("foo")).get("bar"));
        assertEquals(1, wfs.resourcesCreated().size());
        ResourceCreated rc = wfs.resourcesCreated().get(0);
        assertEquals("name", rc.workflowStepName());
        assertEquals("stepId", rc.workflowStepId());
        assertEquals("type", rc.resourceType());
        assertEquals("id", rc.resourceId());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wfs.writeTo(out);
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                wfs = new WorkflowState(in);

                assertEquals(workflowId, wfs.getWorkflowId());
                assertEquals(error, wfs.getError());
                assertEquals(state, wfs.getState());
                assertEquals(provisioningProgress, wfs.getProvisioningProgress());
                assertEquals(provisionStartTime, wfs.getProvisionStartTime());
                assertEquals(provisionEndTime, wfs.getProvisionEndTime());
                assertEquals(1, wfs.userOutputs().size());
                assertEquals("baz", ((Map<?, ?>) wfs.userOutputs().get("foo")).get("bar"));
                assertEquals(1, wfs.resourcesCreated().size());
                rc = wfs.resourcesCreated().get(0);
                assertEquals("name", rc.workflowStepName());
                assertEquals("stepId", rc.workflowStepId());
                assertEquals("type", rc.resourceType());
                assertEquals("id", rc.resourceId());
            }
        }
    }

}
