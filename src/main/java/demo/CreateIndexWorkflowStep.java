/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowInputData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.List;
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
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        logger.debug("Executing with {} input args.", data.size());
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (inputData != null) {
                logger.debug("Initialized params: {}, content: {}", inputData.getParams(), inputData.getContent());
            }
            for (WorkflowData wfData : data) {
                Map<String, Object> content = wfData.getContent();
                if (wfData instanceof WorkflowInputData) {
                    logger.debug(
                        "Previous step sent params: {}, content: {}",
                        ((WorkflowInputData) wfData).getParams(),
                        wfData.getContent()
                    );
                } else {
                    logger.debug("Received from previous step: content: {}", content);
                }
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
