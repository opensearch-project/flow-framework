/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class WorkflowDataStepTests extends OpenSearchTestCase {

    private WorkflowDataStep workflowDataStep;
    private WorkflowData inputData;
    private WorkflowData outputData;

    private String workflowId = "test-id";
    private String workflowStepId = "test-node-id";
    private String resourceId = "resourceId";
    private String resourceType = "resourceType";

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ResourceCreated resourceCreated = new ResourceCreated("step_name", workflowStepId, resourceType, resourceId);
        this.workflowDataStep = new WorkflowDataStep(resourceCreated);
        this.inputData = new WorkflowData(Map.of(), workflowId, workflowStepId);
        this.outputData = new WorkflowData(Map.ofEntries(Map.entry(resourceType, resourceId)), workflowId, workflowStepId);
    }

    public void testExecuteWorkflowDataStep() throws ExecutionException, InterruptedException {

        @SuppressWarnings("unchecked")
        PlainActionFuture<WorkflowData> future = workflowDataStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertTrue(future.isDone());
        assertEquals(outputData.getContent().get(resourceType), future.get().getContent().get(resourceType));

    }

}
