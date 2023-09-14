/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowInputData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CreateIndexWorkflowStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexWorkflowStep.class);

    private final String name;
    private WorkflowInputData inputData = null;

    public CreateIndexWorkflowStep() {
        this.name = "CREATE_INDEX";
    }

    @Override
    public CompletableFuture<WorkflowData> execute(WorkflowData... data) {
        logger.debug("Executing with " + data.length + " args");
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (inputData != null) {
                Map<String, String> params = inputData.getParams();
                Map<String, Object> content = inputData.getContent();
                logger.debug("Initialized params: {}, content: {}", params, content);
            }
            for (WorkflowData wfData : data) {
                Map<String, Object> content = wfData.getContent();
                logger.debug("Received content from previous step: {}", content);
            }
            // do some work
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
            // Simulate response of created index
            CreateIndexResponse response = new CreateIndexResponse(true, true, inputData.getParams().get("index"));
            future.complete(new CreateIndexResponseData(response));
        });

        return future;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setInput(WorkflowInputData data) {
        this.inputData = data;
    }
}
