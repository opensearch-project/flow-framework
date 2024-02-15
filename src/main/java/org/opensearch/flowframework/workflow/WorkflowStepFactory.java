/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Generates instances implementing {@link WorkflowStep}.
 */
public class WorkflowStepFactory {

    private final Map<String, Supplier<WorkflowStep>> stepMap = new HashMap<>();
    private static final Logger logger = LogManager.getLogger(WorkflowStepFactory.class);

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

    /**
     * Enum encapsulating the different step names, their inputs, outputs, required plugin and timeout of the step
     */

    public enum WorkflowSteps {

        NOOP("noop", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null),

        CREATE_CONNECTOR(
            "create_connector",
            Arrays.asList("name", "description", "version", "protocol", "parameters", "credential", "actions"),
            Arrays.asList("connector_id"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(60, SECONDS)
        ),

        REGISTER_LOCAL_CUSTOM_MODEL(
            "register_local_custom_model",
            Arrays.asList(
                "name",
                "version",
                "model_format",
                "function_name",
                "model_content_hash_value",
                "url",
                "model_type",
                "embedding_dimension",
                "framework_type"
            ),
            Arrays.asList("model_id", "register_model_status"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(60, SECONDS)
        ),

        REGISTER_LOCAL_SPARSE_ENCODING_MODEL(
            "register_local_sparse_encoding_model",
            Arrays.asList("name", "version", "model_format", "function_name", "model_content_hash_value", "url"),
            Arrays.asList("model_id", "register_model_status"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(60, SECONDS)
        ),
        REGISTER_LOCAL_PRETRAINED_MODEL(
            "register_local_pretrained_model",
            Arrays.asList("name", "version", "model_format"),
            Arrays.asList("model_id", "register_model_status"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(60, SECONDS)
        ),

        REGISTER_REMOTE_MODEL(
            "register_remote_model",
            Arrays.asList("name", "connector_id"),
            Arrays.asList("model_id", "register_model_status"),
            Arrays.asList("opensearch-ml"),
            null
        ),

        REGISTER_MODEL_GROUP(
            "register_model_group",
            Arrays.asList("name"),
            Arrays.asList("model_group_id", "model_group_status"),
            Arrays.asList("opensearch-ml"),
            null
        ),

        DEPLOY_MODEL(
            "deploy_model",
            Arrays.asList("model_id"),
            Arrays.asList("deploy_model_status"),
            Arrays.asList("opensearch-ml"),
            new TimeValue(15, SECONDS)
        ),

        UNDEPLOY_MODEL("undeploy_model", Arrays.asList("model_id"), Arrays.asList("success"), Arrays.asList("opensearch-ml"), null),

        DELETE_MODEL("delete_model", Arrays.asList("model_id"), Arrays.asList("model_id"), Arrays.asList("opensearch-ml"), null),

        DELETE_CONNECTOR(
            "delete_connector",
            Arrays.asList("connector_id"),
            Arrays.asList("connector_id"),
            Arrays.asList("opensearch-ml"),
            null
        ),

        REGISTER_AGENT("register_agent", Arrays.asList("name", "type"), Arrays.asList("agent_id"), Arrays.asList("opensearch-ml"), null),

        DELETE_AGENT("delete_agent", Arrays.asList("agent_id"), Arrays.asList("agent_id"), Arrays.asList("opensearch-ml"), null),

        CREATE_TOOL("create_tool", Arrays.asList("type"), Arrays.asList("tools"), Arrays.asList("opensearch-ml"), null);

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

        /**
         * Gets the timeout based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static TimeValue getTimeoutByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (workflowStep != null && !workflowStep.isEmpty()) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStep())) {
                        return mapping.getTimeout();
                    }
                }
            }
            logger.error("Unable to find workflow timeout for step: {}", workflowStep);
            throw new FlowFrameworkException("Unable to find workflow timeout for step: " + workflowStep, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the required plugins based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getRequiredPluginsByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (workflowStep != null && !workflowStep.isEmpty()) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStep())) {
                        return mapping.getRequiredPlugins();
                    }
                }
            }
            logger.error("Unable to find workflow required plugins for step: {}", workflowStep);
            throw new FlowFrameworkException("Unable to find workflow required plugins for step: " + workflowStep, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the output based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getOutputByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (workflowStep != null && !workflowStep.isEmpty()) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStep())) {
                        return mapping.getOutputs();
                    }
                }
            }
            logger.error("Unable to find workflow output for step: {}", workflowStep);
            throw new FlowFrameworkException("Unable to find workflow output for step: " + workflowStep, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the input based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getInputByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (workflowStep != null && !workflowStep.isEmpty()) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStep())) {
                        return mapping.getInputs();
                    }
                }
            }
            logger.error("Unable to find workflow input for step: {}", workflowStep);
            throw new FlowFrameworkException("Unable to find workflow input for step: " + workflowStep, RestStatus.BAD_REQUEST);
        }

    }

    /**
     * Get the object of WorkflowValidator consisting of workflow steps
     * @return WorkflowValidator
     */
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
