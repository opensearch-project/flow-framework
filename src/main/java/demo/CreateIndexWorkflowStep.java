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
        // TODO we will be passing a thread pool to this object when it's instantiated
        // we should either add the generic executor from that pool to this call
        // or use executorservice.submit or any of various threading options
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/issues/42
        CompletableFuture.runAsync(() -> {
            String inputIndex = null;
            for (WorkflowData wfData : data) {
                logger.debug(
                    "{} sent params: {}, content: {}",
                    inputIndex == null ? "Initialization" : "Previous step",
                    wfData.getParams(),
                    wfData.getContent()
                );
                if (inputIndex == null) {
                    inputIndex = wfData.getParams()
                        .getOrDefault("index_name", (String) wfData.getContent().getOrDefault("index_name", "NOT FOUND"));
                }
            }
            // do some work, simulating a REST API call
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {}
            // Simulate response of created index
            CreateIndexResponse response = new CreateIndexResponse(true, true, inputIndex);
            future.complete(new WorkflowData(Map.of("index", response.index())));
        });

        return future;
    }

    @Override
    public String getName() {
        return name;
    }
}
