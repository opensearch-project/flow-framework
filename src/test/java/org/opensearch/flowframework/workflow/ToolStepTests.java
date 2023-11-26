/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ToolStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

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
            )
        );
    }

    public void testTool() throws IOException, ExecutionException, InterruptedException {
        ToolStep toolStep = new ToolStep();

        CompletableFuture<WorkflowData> future = toolStep.execute(List.of(inputData));

        assertTrue(future.isDone());
        assertEquals(MLToolSpec.class, future.get().getContent().get("tools").getClass());
    }
}
