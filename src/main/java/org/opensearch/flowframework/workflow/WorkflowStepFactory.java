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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowStepValidator;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private final Map<String, Supplier<WorkflowStep>> stepMap = new HashMap<>();

    /**
     * Instantiate this class.
     *
     * @param threadPool The OpenSearch thread pool
     * @param clusterService The OpenSearch cluster service
     * @param client The OpenSearch client steps can use
     * @param mlClient Machine Learning client to perform ml operations
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings common settings of the plugin
     */
    public WorkflowStepFactory(
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        stepMap.put(NoOpStep.NAME, NoOpStep::new);
        stepMap.put(
            RegisterLocalCustomModelStep.NAME,
            () -> new RegisterLocalCustomModelStep(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings)
        );
        stepMap.put(
            RegisterLocalSparseEncodingModelStep.NAME,
            () -> new RegisterLocalSparseEncodingModelStep(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings)
        );
        stepMap.put(
            RegisterLocalPretrainedModelStep.NAME,
            () -> new RegisterLocalPretrainedModelStep(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings)
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
        stepMap.put(RegisterModelGroupStep.NAME, () -> new RegisterModelGroupStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(ToolStep.NAME, ToolStep::new);
        stepMap.put(RegisterAgentStep.NAME, () -> new RegisterAgentStep(mlClient, flowFrameworkIndicesHandler));
        stepMap.put(DeleteAgentStep.NAME, () -> new DeleteAgentStep(mlClient));
    }

    public enum WorkflowSteps {
        CREATE_CONNECTOR(
            "create_connector",
            Arrays.asList("name ", "description", "version", "protocol", "parameters", "credential", "actions"),
            Arrays.asList("connector_id"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(60, SECONDS)
        ),

        REGISTER_REMOTE_MODEL(
            "register_remote_model",
            Arrays.asList("name ", "connector_id"),
            Arrays.asList("model_id", "register_model_status"),
            Arrays.asList("opensearch-ml"),
            null
        ),

        DELETE_CONNECTOR(
            "delete_connector",
            Arrays.asList("connector_id"),
            Arrays.asList("connector_id"),
            Arrays.asList("opensearch-ml"),
            null
        );

        private final String workflowStep;
        private final List<String> inputs;
        private final List<String> outputs;
        private final List<String> requiredPlugins;
        private final TimeValue timeout;

        private static final List<String> allWorkflowSteps = Stream.of(values())
            .map(WorkflowSteps::getWorkflowStep)
            .collect(Collectors.toList());

        WorkflowSteps(String workflowStep, List<String> inputs, List<String> outputs, List<String> requiredPlugins, TimeValue timeout) {
            this.workflowStep = workflowStep;
            this.inputs = List.copyOf(inputs);
            this.outputs = List.copyOf(outputs);
            this.requiredPlugins = requiredPlugins;
            this.timeout = timeout;
        }

        /**
         * Returns the workflowStep for the given enum Constant
         * @return the workflowStep of this data.
         */
        public String getWorkflowStep() {
            return workflowStep;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public List<String> getOutputs() {
            return outputs;
        }

        public List<String> getRequiredPlugins() {
            return requiredPlugins;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public WorkflowStepValidator getWorkflowStepValidator() {
            return new WorkflowStepValidator(workflowStep, inputs, outputs, requiredPlugins, timeout);
        };

    }

    public WorkflowValidator getWorkflowValidator() {
        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();

        for (WorkflowSteps mapping : WorkflowSteps.values()) {
            workflowStepValidators.put(mapping.getWorkflowStep(), mapping.getWorkflowStepValidator());
        }

        return new WorkflowValidator(workflowStepValidators);
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
