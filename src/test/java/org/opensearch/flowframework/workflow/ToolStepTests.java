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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

public class ToolStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;
    private WorkflowData inputDataWithConnectorId;
    private WorkflowData inputDataWithModelId;
    private WorkflowData inputDataWithAgentId;
    private static final String mockedConnectorId = "mocked-connector-id";
    private static final String mockedModelId = "mocked-model-id";
    private static final String mockedAgentId = "mocked-agent-id";

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
        inputDataWithConnectorId = new WorkflowData(Map.of(CONNECTOR_ID, mockedConnectorId), "test-id", "test-node-id");
        inputDataWithModelId = new WorkflowData(Map.of(MODEL_ID, mockedModelId), "test-id", "test-node-id");
        inputDataWithAgentId = new WorkflowData(Map.of(AGENT_ID, mockedAgentId), "test-id", "test-node-id");
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

    public void testTool() throws ExecutionException, InterruptedException {
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

        toolStep = new ToolStep();
        future = toolStep.execute(
            boolStringInputData.getNodeId(),
            boolStringInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());
    }

    public void testBoolParseFail() {
        ToolStep toolStep = new ToolStep();

        PlainActionFuture<WorkflowData> future = toolStep.execute(
            badBoolInputData.getNodeId(),
            badBoolInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, future::get);
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [yes] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }

    public void testToolWithConnectorId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        String createConnectorNodeName = "create_connector";
        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createConnectorNodeName, inputDataWithConnectorId),
            Map.of(createConnectorNodeName, CONNECTOR_ID),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(CONNECTOR_ID, mockedConnectorId));
    }

    public void testToolWithModelId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        String createModelNodeName = "create_model";
        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createModelNodeName, inputDataWithModelId),
            Map.of(createModelNodeName, MODEL_ID),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(MODEL_ID, mockedModelId));
    }

    public void testToolWithAgentId() throws ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        String createAgentNodeName = "create_agent";
        PlainActionFuture<WorkflowData> future = toolStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of(createAgentNodeName, inputDataWithAgentId),
            Map.of(createAgentNodeName, AGENT_ID),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        Object tools = future.get().getContent().get("tools");
        assertEquals(MLToolSpec.class, tools.getClass());
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        assertEquals(mlToolSpec.getParameters(), Map.of(AGENT_ID, mockedAgentId));
    }
}
