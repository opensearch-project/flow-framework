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
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
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
        User user = new User("user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        Map<String, Object> userOutputs = Map.of("foo", Map.of("bar", "baz"));
        List<ResourceCreated> resourcesCreated = List.of(new ResourceCreated("name", "stepId", "type", "id"));

        WorkflowState wfs = WorkflowState.builder()
            .workflowId(workflowId)
            .error(error)
            .state(state)
            .provisioningProgress(provisioningProgress)
            .provisionStartTime(provisionStartTime)
            .provisionEndTime(provisionEndTime)
            .user(user)
            .userOutputs(userOutputs)
            .resourcesCreated(resourcesCreated)
            .build();

        assertEquals(workflowId, wfs.getWorkflowId());
        assertEquals(error, wfs.getError());
        assertEquals(state, wfs.getState());
        assertEquals(provisioningProgress, wfs.getProvisioningProgress());
        assertEquals(provisionStartTime, wfs.getProvisionStartTime());
        assertEquals(provisionEndTime, wfs.getProvisionEndTime());
        assertEquals("user", wfs.getUser().getName());
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
                assertEquals("user", wfs.getUser().getName());
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

    public void testWorkflowStateUpdate() {
        // Time travel to guarantee update increments
        Instant now = Instant.now().minusMillis(100);

        WorkflowState wfs = WorkflowState.builder()
            .workflowId("1")
            .error("error one")
            .state("state one")
            .provisioningProgress("progress one")
            .provisionStartTime(now)
            .provisionEndTime(now)
            .user(new User("one", Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            .userOutputs(Map.of("output", "one"))
            .resourcesCreated(List.of(new ResourceCreated("", "", "", "id one")))
            .build();

        assertEquals("1", wfs.getWorkflowId());
        assertEquals("error one", wfs.getError());
        assertEquals("state one", wfs.getState());
        assertEquals("progress one", wfs.getProvisioningProgress());
        assertEquals(now, wfs.getProvisionStartTime());
        assertEquals(now, wfs.getProvisionEndTime());
        assertEquals("one", wfs.getUser().getName());
        assertEquals(1, wfs.userOutputs().size());
        assertEquals("one", wfs.userOutputs().get("output"));
        assertEquals(1, wfs.resourcesCreated().size());
        ResourceCreated rc = wfs.resourcesCreated().get(0);
        assertEquals("id one", rc.resourceId());

        WorkflowState update = WorkflowState.builder()
            .workflowId("2")
            .error("error two")
            .state("state two")
            .provisioningProgress("progress two")
            .user(new User("two", Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))
            .build();

        wfs = WorkflowState.updateExistingWorkflowState(wfs, update);
        assertEquals("2", wfs.getWorkflowId());
        assertEquals("error two", wfs.getError());
        assertEquals("state two", wfs.getState());
        assertEquals("progress two", wfs.getProvisioningProgress());
        assertEquals(now, wfs.getProvisionStartTime());
        assertEquals(now, wfs.getProvisionEndTime());
        assertEquals("two", wfs.getUser().getName());
        assertEquals(1, wfs.userOutputs().size());
        assertEquals("one", wfs.userOutputs().get("output"));
        assertEquals(1, wfs.resourcesCreated().size());
        rc = wfs.resourcesCreated().get(0);
        assertEquals("id one", rc.resourceId());

        now = Instant.now().minusMillis(100);
        update = WorkflowState.builder()
            .provisionStartTime(now)
            .provisionEndTime(now)
            .userOutputs(Map.of("output", "two"))
            .resourcesCreated(List.of(wfs.resourcesCreated().get(0), new ResourceCreated("", "", "", "id two")))
            .build();

        wfs = WorkflowState.updateExistingWorkflowState(wfs, update);
        assertEquals("2", wfs.getWorkflowId());
        assertEquals("error two", wfs.getError());
        assertEquals("state two", wfs.getState());
        assertEquals("progress two", wfs.getProvisioningProgress());
        assertEquals(now, wfs.getProvisionStartTime());
        assertEquals(now, wfs.getProvisionEndTime());
        assertEquals("two", wfs.getUser().getName());
        assertEquals(1, wfs.userOutputs().size());
        assertEquals("two", wfs.userOutputs().get("output"));
        assertEquals(2, wfs.resourcesCreated().size());
        rc = wfs.resourcesCreated().get(1);
        assertEquals("id two", rc.resourceId());
    }
}
