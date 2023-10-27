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
import org.opensearch.ml.client.MachineLearningNodeClient;

import java.util.HashMap;
import java.util.Map;

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
     * @param mlClient Machine Learning client to perform ml operations
     */

    public WorkflowStepFactory(ClusterService clusterService, Client client, MachineLearningNodeClient mlClient) {
        populateMap(clusterService, client, mlClient);
    }

    private void populateMap(ClusterService clusterService, Client client, MachineLearningNodeClient mlClient) {
        stepMap.put(CreateIndexStep.NAME, new CreateIndexStep(clusterService, client));
        stepMap.put(CreateIngestPipelineStep.NAME, new CreateIngestPipelineStep(client));
        stepMap.put(RegisterModelStep.NAME, new RegisterModelStep(mlClient));
        stepMap.put(DeployModelStep.NAME, new DeployModelStep(mlClient));
        stepMap.put(CreateConnectorStep.NAME, new CreateConnectorStep(mlClient));
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

    /**
     * Gets the step map
     * @return the step map
     */
    public Map<String, WorkflowStep> getStepMap() {
        return this.stepMap;
    }
}
