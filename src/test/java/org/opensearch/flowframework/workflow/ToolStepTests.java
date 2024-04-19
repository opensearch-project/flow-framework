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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ToolStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;
    private WorkflowData boolStringInputData;
    private WorkflowData badBoolInputData;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("include_output_in_agent_response", false)
            ),
            "test-id",
            "test-node-id"
        );
        boolStringInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("include_output_in_agent_response", "false")
            ),
            "test-id",
            "test-node-id"
        );
        badBoolInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("type", "type"),
                Map.entry("name", "name"),
                Map.entry("description", "description"),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("include_output_in_agent_response", "yes")
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testTool() throws IOException, ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());
    }

    public void testBoolParse() throws IOException, ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            boolStringInputData.getNodeId(),
            boolStringInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());
    }

    public void testBoolParseFail() throws IOException, ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            badBoolInputData.getNodeId(),
            badBoolInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [yes] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }
}
