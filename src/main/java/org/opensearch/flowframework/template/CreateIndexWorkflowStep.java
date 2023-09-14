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
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CreateIndexWorkflowStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexWorkflowStep.class);

    private final String name;
    private WorkflowData data = null;

    public CreateIndexWorkflowStep() {
        this.name = "CREATE_INDEX";
    }

    @Override
    public CompletableFuture<WorkflowData> execute(WorkflowData data) {
        // TODO, need to handle this better, we are pre-running execute all the time so passing a param is silly
        if (data != null) {
            this.data = data;
        }
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (this.data instanceof CreateIndexRequestData) {
                Map<String, String> params = ((CreateIndexRequestData) this.data).getParams();
                Map<String, Object> content = ((CreateIndexRequestData) this.data).getContent();
                logger.debug("Received params: {}, content: {}", params, content);
                future.complete(null);
            } else {
                logger.debug("Wrong data type!");
                future.completeExceptionally(new IllegalArgumentException("Expected CreateIndexRequestData here."));
            }
        });
        return future;
    }

    @Override
    public String getName() {
        return name;
    }

    public WorkflowData getData() {
        return data;
    }

    public void setData(WorkflowData data) {
        this.data = data;
    }
}
