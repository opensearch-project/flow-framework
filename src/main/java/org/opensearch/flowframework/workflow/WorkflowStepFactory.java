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
import org.opensearch.cluster.service.ClusterService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import demo.DemoWorkflowStep;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private final Map<String, WorkflowStep> stepMap = new HashMap<>();

    /**
     * Instantiate this class.
     *
     * @param clusterService The OpenSearch cluster service
     * @param client The OpenSearch client steps can use
     */

    public WorkflowStepFactory(ClusterService clusterService, Client client) {
        populateMap(clusterService, client);
    }

    private void populateMap(ClusterService clusterService, Client client) {
        stepMap.put(CreateIndexStep.NAME, new CreateIndexStep(clusterService, client));
        stepMap.put(CreateIngestPipelineStep.NAME, new CreateIngestPipelineStep(client));
        stepMap.put(RegisterModelStep.NAME, new RegisterModelStep(client));
        stepMap.put(DeployModelStep.NAME, new DeployModelStep(client));

        // TODO: These are from the demo class as placeholders, remove when demos are deleted
        stepMap.put("demo_delay_3", new DemoWorkflowStep(3000));
        stepMap.put("demo_delay_5", new DemoWorkflowStep(5000));

        // Use as a default until all the actual implementations are ready
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
