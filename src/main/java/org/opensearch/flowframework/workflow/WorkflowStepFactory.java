/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import demo.CreateIndexWorkflowStep;
import demo.DemoWorkflowStep;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private static final WorkflowStepFactory INSTANCE = new WorkflowStepFactory();

    private final Map<String, WorkflowStep> stepMap = new HashMap<>();

    private WorkflowStepFactory() {
        populateMap();
    }

    /**
     * Gets the singleton instance of this class
     * @return The instance of this class
     */
    public static WorkflowStepFactory get() {
        return INSTANCE;
    }

    private void populateMap() {
        // TODO: These are from the demo class as placeholders
        // Replace with actual implementations such as
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/pull/38
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/pull/44
        stepMap.put("create_index", new CreateIndexWorkflowStep());
        stepMap.put("fetch_model", new DemoWorkflowStep(3000));
        stepMap.put("create_ingest_pipeline", new DemoWorkflowStep(3000));
        stepMap.put("create_search_pipeline", new DemoWorkflowStep(5000));
        stepMap.put("create_neural_search_index", new DemoWorkflowStep(2000));

        // Use until all the actual implementations are ready
        stepMap.put("placeholder", new WorkflowStep() {
            @Override
            public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
                CompletableFuture<WorkflowData> future = new CompletableFuture<>();
                future.complete(WorkflowData.EMPTY);
                return future;
            }

            @Override
            public String getName() {
                return "placeholder";
            }
        });
    }

    /**
     * Create a new instance of a {@link WorkflowStep}.
     * @param type The type of instance to create
     * @return an instance of the specified type
     */
    public WorkflowStep createStep(String type) {
        if (stepMap.containsKey(type)) {
            return stepMap.get(type);
        }
        // TODO: replace this with a FlowFrameworkException
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/pull/43
        return stepMap.get("placeholder");
    }
}
