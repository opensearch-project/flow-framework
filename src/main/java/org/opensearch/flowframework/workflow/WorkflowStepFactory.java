/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.client.Client;
import org.opensearch.flowframework.workflow.CreateIndex.CreateIndexStep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import demo.DemoWorkflowStep;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private static WorkflowStepFactory instance = null;

    private Client client;
    private final Map<String, WorkflowStep> stepMap = new HashMap<>();

    /**
     * Create the singleton instance of this class. Throws an {@link IllegalStateException} if already created.
     *
     * @param client The OpenSearch client steps can use
     * @return The created instance
     */
    public static synchronized WorkflowStepFactory create(Client client) {
        if (instance != null) {
            throw new IllegalStateException("This factory was already created.");
        }
        instance = new WorkflowStepFactory(client);
        return instance;
    }

    /**
     * Gets the singleton instance of this class. Throws an {@link IllegalStateException} if not yet created.
     *
     * @return The created instance
     */
    public static synchronized WorkflowStepFactory get() {
        if (instance == null) {
            throw new IllegalStateException("This factory has not yet been created.");
        }
        return instance;
    }

    private WorkflowStepFactory(Client client) {
        this.client = client;
        populateMap();
    }

    private void populateMap() {
        stepMap.put(CreateIndexStep.NAME, new CreateIndexStep(client));

        // TODO: These are from the demo class as placeholders
        // Replace with actual implementations such as
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/pull/38
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/pull/44
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
