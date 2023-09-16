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
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sample to show other devs how to pass data around.  Will be deleted once other PRs are merged.
 */
public class CreateIndexWorkflowStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexWorkflowStep.class);

    private final String name;

    /**
     * Instantiate this class.
     */
    public CreateIndexWorkflowStep() {
        this.name = "CREATE_INDEX";
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            String inputIndex = null;
            boolean first = true;
            for (WorkflowData wfData : data) {
                logger.debug(
                    "{} sent params: {}, content: {}",
                    first ? "Initialization" : "Previous step",
                    wfData.getParams(),
                    wfData.getContent()
                );
                if (first) {
                    Map<String, String> params = data.get(0).getParams();
                    if (params.containsKey("index")) {
                        inputIndex = params.get("index");
                    }
                    first = false;
                }
            }
            // do some work, simulating a REST API call
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
            // Simulate response of created index
            CreateIndexResponse response = new CreateIndexResponse(true, true, inputIndex);
            // OLD UNSCALABLE WAY: future.complete(new CreateIndexResponseData(response));
            // Better way with an anonymous class:
            future.complete(new WorkflowData() {
                @Override
                public Map<String, Object> getContent() {
                    return Map.of("index", response.index());
                }
            });
        });

        return future;
    }

    @Override
    public String getName() {
        return name;
    }
}
