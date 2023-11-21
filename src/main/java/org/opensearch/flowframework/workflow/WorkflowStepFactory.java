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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private final Map<String, WorkflowStep> stepMap = new HashMap<>();
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiate this class.
     *
     * @param settings The OpenSearch settings
     * @param clusterService The OpenSearch cluster service
     * @param client The OpenSearch client steps can use
     * @param mlClient Machine Learning client to perform ml operations
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public WorkflowStepFactory(
        Settings settings,
        ClusterService clusterService,
        Client client,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        populateMap(settings, clusterService, client, mlClient, flowFrameworkIndicesHandler);
    }

    private void populateMap(
        Settings settings,
        ClusterService clusterService,
        Client client,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        stepMap.put(NoOpStep.NAME, new NoOpStep());
        stepMap.put(CreateIndexStep.NAME, new CreateIndexStep(clusterService, client));
        stepMap.put(CreateIngestPipelineStep.NAME, new CreateIngestPipelineStep(client));
        stepMap.put(RegisterLocalModelStep.NAME, new RegisterLocalModelStep(mlClient));
        stepMap.put(RegisterRemoteModelStep.NAME, new RegisterRemoteModelStep(mlClient));
        stepMap.put(DeployModelStep.NAME, new DeployModelStep(mlClient));
        stepMap.put(CreateConnectorStep.NAME, new CreateConnectorStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(ModelGroupStep.NAME, new ModelGroupStep(mlClient));
        stepMap.put(GetMLTaskStep.NAME, new GetMLTaskStep(settings, clusterService, mlClient));
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
        throw new FlowFrameworkException("Workflow step type [" + type + "] is not implemented.", RestStatus.NOT_IMPLEMENTED);
    }

    /**
     * Gets the step map
     * @return the step map
     */
    public Map<String, WorkflowStep> getStepMap() {
        return Map.copyOf(this.stepMap);
    }
}
