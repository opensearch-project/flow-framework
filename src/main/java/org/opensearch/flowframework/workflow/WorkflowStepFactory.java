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
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private final Map<String, Supplier<WorkflowStep>> stepMap = new HashMap<>();

    /**
     * Instantiate this class.
     *
     * @param settings The OpenSearch settings
     * @param threadPool The OpenSearch thread pool
     * @param clusterService The OpenSearch cluster service
     * @param client The OpenSearch client steps can use
     * @param mlClient Machine Learning client to perform ml operations
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public WorkflowStepFactory(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        stepMap.put(NoOpStep.NAME, NoOpStep::new);
        stepMap.put(CreateIndexStep.NAME, () -> new CreateIndexStep(clusterService, client, flowFrameworkIndicesHandler));
        stepMap.put(CreateIngestPipelineStep.NAME, () -> new CreateIngestPipelineStep(client, flowFrameworkIndicesHandler));
        stepMap.put(
            RegisterLocalModelStep.NAME,
            () -> new RegisterLocalModelStep(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings)
        );
        stepMap.put(RegisterRemoteModelStep.NAME, () -> new RegisterRemoteModelStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(DeleteModelStep.NAME, () -> new DeleteModelStep(mlClient));
        stepMap.put(
            DeployModelStep.NAME,
            () -> new DeployModelStep(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings)
        );
        stepMap.put(UndeployModelStep.NAME, () -> new UndeployModelStep(mlClient));
        stepMap.put(CreateConnectorStep.NAME, () -> new CreateConnectorStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(DeleteConnectorStep.NAME, () -> new DeleteConnectorStep(mlClient));
        stepMap.put(ModelGroupStep.NAME, () -> new ModelGroupStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(ToolStep.NAME, ToolStep::new);
        stepMap.put(RegisterAgentStep.NAME, () -> new RegisterAgentStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(DeleteAgentStep.NAME, () -> new DeleteAgentStep(mlClient));
    }

    /**
     * Create a new instance of a {@link WorkflowStep}.
     * @param type The type of instance to create
     * @return an instance of the specified type
     */
    public WorkflowStep createStep(String type) {
        if (stepMap.containsKey(type)) {
            return stepMap.get(type).get();
        }
        throw new FlowFrameworkException("Workflow step type [" + type + "] is not implemented.", RestStatus.NOT_IMPLEMENTED);
    }

    /**
     * Gets the step map
     * @return a read-only copy of the step map
     */
    public Map<String, Supplier<WorkflowStep>> getStepMap() {
        return Map.copyOf(this.stepMap);
    }
}
