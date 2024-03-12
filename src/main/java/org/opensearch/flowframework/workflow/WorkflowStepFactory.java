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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowStepValidator;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.flowframework.common.CommonValue.ACTIONS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.EMBEDDING_DIMENSION;
import static org.opensearch.flowframework.common.CommonValue.FRAMEWORK_TYPE;
import static org.opensearch.flowframework.common.CommonValue.FUNCTION_NAME;
import static org.opensearch.flowframework.common.CommonValue.MODEL_CONTENT_HASH_VALUE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_STATUS;
import static org.opensearch.flowframework.common.CommonValue.MODEL_TYPE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.OPENSEARCH_ML;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PIPELINE_ID;
import static org.opensearch.flowframework.common.CommonValue.PROTOCOL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.SUCCESS;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.CommonValue.URL;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

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
     * @param mlClient Machine Learning client to perform ml operations
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings common settings of the plugin
     * @param client The OpenSearch Client
     */
    public WorkflowStepFactory(
        ThreadPool threadPool,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        Client client
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
        stepMap.put(CreateIngestPipelineStep.NAME, () -> new CreateIngestPipelineStep(client, flowFrameworkIndicesHandler));
        stepMap.put(CreateSearchPipelineStep.NAME, () -> new CreateSearchPipelineStep(client, flowFrameworkIndicesHandler));
    }

    /**
     * Enum encapsulating the different step names, their inputs, outputs, required plugin and timeout of the step
     */

    public enum WorkflowSteps {

        /** Noop Step */
        NOOP("noop", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null),

        /** Create Connector Step */
        CREATE_CONNECTOR(
            CreateConnectorStep.NAME,
            List.of(NAME_FIELD, DESCRIPTION_FIELD, VERSION_FIELD, PROTOCOL_FIELD, PARAMETERS_FIELD, CREDENTIAL_FIELD, ACTIONS_FIELD),
            List.of(CONNECTOR_ID),
            List.of(OPENSEARCH_ML),
            TimeValue.timeValueSeconds(60)
        ),

        /** Register Local Custom Model Step */
        REGISTER_LOCAL_CUSTOM_MODEL(
            RegisterLocalCustomModelStep.NAME,
            List.of(
                NAME_FIELD,
                VERSION_FIELD,
                MODEL_FORMAT,
                FUNCTION_NAME,
                MODEL_CONTENT_HASH_VALUE,
                URL,
                MODEL_TYPE,
                EMBEDDING_DIMENSION,
                FRAMEWORK_TYPE
            ),
            List.of(MODEL_ID, REGISTER_MODEL_STATUS),
            List.of(OPENSEARCH_ML),
            TimeValue.timeValueSeconds(60)
        ),

        /** Register Local Sparse Encoding Model Step */
        REGISTER_LOCAL_SPARSE_ENCODING_MODEL(
            RegisterLocalSparseEncodingModelStep.NAME,
            List.of(NAME_FIELD, VERSION_FIELD, MODEL_FORMAT, FUNCTION_NAME, MODEL_CONTENT_HASH_VALUE, URL),
            List.of(MODEL_ID, REGISTER_MODEL_STATUS),
            List.of(OPENSEARCH_ML),
            TimeValue.timeValueSeconds(60)
        ),

        /** Register Local Pretrained Model Step */
        REGISTER_LOCAL_PRETRAINED_MODEL(
            RegisterLocalPretrainedModelStep.NAME,
            List.of(NAME_FIELD, VERSION_FIELD, MODEL_FORMAT),
            List.of(MODEL_ID, REGISTER_MODEL_STATUS),
            List.of(OPENSEARCH_ML),
            TimeValue.timeValueSeconds(60)
        ),

        /** Register Remote Model Step */
        REGISTER_REMOTE_MODEL(
            RegisterRemoteModelStep.NAME,
            List.of(NAME_FIELD, CONNECTOR_ID),
            List.of(MODEL_ID, REGISTER_MODEL_STATUS),
            List.of(OPENSEARCH_ML),
            null
        ),

        /** Register Model Group Step */
        REGISTER_MODEL_GROUP(
            RegisterModelGroupStep.NAME,
            List.of(NAME_FIELD),
            List.of(MODEL_GROUP_ID, MODEL_GROUP_STATUS),
            List.of(OPENSEARCH_ML),
            null
        ),

        /** Deploy Model Step */
        DEPLOY_MODEL(DeployModelStep.NAME, List.of(MODEL_ID), List.of(MODEL_ID), List.of(OPENSEARCH_ML), TimeValue.timeValueSeconds(15)),

        /** Undeploy Model Step */
        UNDEPLOY_MODEL(UndeployModelStep.NAME, List.of(MODEL_ID), List.of(SUCCESS), List.of(OPENSEARCH_ML), null),

        /** Delete Model Step */
        DELETE_MODEL(DeleteModelStep.NAME, List.of(MODEL_ID), List.of(MODEL_ID), List.of(OPENSEARCH_ML), null),

        /** Delete Connector Step */
        DELETE_CONNECTOR(DeleteConnectorStep.NAME, List.of(CONNECTOR_ID), List.of(CONNECTOR_ID), List.of(OPENSEARCH_ML), null),

        /** Register Agent Step */
        REGISTER_AGENT(RegisterAgentStep.NAME, List.of(NAME_FIELD, TYPE), List.of(AGENT_ID), List.of(OPENSEARCH_ML), null),

        /** Delete Agent Step */
        DELETE_AGENT(DeleteAgentStep.NAME, List.of(AGENT_ID), List.of(AGENT_ID), List.of(OPENSEARCH_ML), null),

        /** Create Tool Step */
        CREATE_TOOL(ToolStep.NAME, List.of(TYPE), List.of(TOOLS_FIELD), List.of(OPENSEARCH_ML), null),

        /** Create Ingest Pipeline Step */
        CREATE_INGEST_PIPELINE(
            CreateIngestPipelineStep.NAME,
            List.of(PIPELINE_ID, CONFIGURATIONS),
            List.of(PIPELINE_ID),
            Collections.emptyList(),
            null
        ),

        /** Create Ingest Pipeline Step */
        CREATE_SEARCH_PIPELINE(
            CreateSearchPipelineStep.NAME,
            List.of(PIPELINE_ID, CONFIGURATIONS),
            List.of(PIPELINE_ID),
            Collections.emptyList(),
            null
        );

        private final String workflowStepName;
        private final List<String> inputs;
        private final List<String> outputs;
        private final List<String> requiredPlugins;
        private final TimeValue timeout;

        WorkflowSteps(String workflowStepName, List<String> inputs, List<String> outputs, List<String> requiredPlugins, TimeValue timeout) {
            this.workflowStepName = workflowStepName;
            this.inputs = List.copyOf(inputs);
            this.outputs = List.copyOf(outputs);
            this.requiredPlugins = requiredPlugins;
            this.timeout = timeout;
        }

        /**
         * Returns the workflowStep for the given enum Constant
         * @return the workflowStep of this data.
         */
        public String getWorkflowStepName() {
            return workflowStepName;
        }

        /**
         * Get the required inputs
         * @return the inputs
         */
        public List<String> inputs() {
            return inputs;
        }

        /**
         * Get the required outputs
         * @return the outputs
         */
        public List<String> outputs() {
            return outputs;
        }

        /**
         * Get the required plugins
         * @return the required plugins
         */
        public List<String> requiredPlugins() {
            return requiredPlugins;
        }

        /**
         * Get the timeout
         * @return the timeout
         */
        public TimeValue timeout() {
            return timeout;
        }

        /**
         * Get the workflow step validator object
         * @return the WorkflowStepValidator
         */
        public WorkflowStepValidator getWorkflowStepValidator() {
            return new WorkflowStepValidator(inputs, outputs, requiredPlugins, timeout);
        }

        /**
         * Gets the timeout based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static TimeValue getTimeoutByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (!Strings.isNullOrEmpty(workflowStep)) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStepName())) {
                        return mapping.timeout();
                    }
                }
            }
            String errorMessage = "Unable to find workflow timeout for step: " + workflowStep;
            logger.error(errorMessage);
            throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the required plugins based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getRequiredPluginsByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (!Strings.isNullOrEmpty(workflowStep)) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStepName())) {
                        return mapping.requiredPlugins();
                    }
                }
            }
            String errorMessage = "Unable to find workflow required plugins for step: " + workflowStep;
            logger.error(errorMessage);
            throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the output based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getOutputByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (!Strings.isNullOrEmpty(workflowStep)) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStepName())) {
                        return mapping.outputs();
                    }
                }
            }
            String errorMessage = "Unable to find workflow output for step " + workflowStep;
            logger.error(errorMessage);
            throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
        }

        /**
         * Gets the input based on the workflowStep.
         * @param workflowStep workflow step type
         * @return the resource that will be created
         * @throws FlowFrameworkException if workflow step doesn't exist in enum
         */
        public static List<String> getInputByWorkflowType(String workflowStep) throws FlowFrameworkException {
            if (!Strings.isNullOrEmpty(workflowStep)) {
                for (WorkflowSteps mapping : values()) {
                    if (workflowStep.equals(mapping.getWorkflowStepName())) {
                        return mapping.inputs();
                    }
                }
            }
            String errorMessage = "Unable to find workflow input for step: " + workflowStep;
            logger.error(errorMessage);
            throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
        }

    }

    /**
     * Get the object of WorkflowValidator consisting of workflow steps
     * @return WorkflowValidator
     */
    public WorkflowValidator getWorkflowValidator() {
        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();

        for (WorkflowSteps mapping : WorkflowSteps.values()) {
            workflowStepValidators.put(mapping.getWorkflowStepName(), mapping.getWorkflowStepValidator());
        }

        return new WorkflowValidator(workflowStepValidators);
    }

    /**
     * Get the object of WorkflowValidator consisting of passed workflow steps
     * @param steps workflow steps
     * @return WorkflowValidator
     */
    public WorkflowValidator getWorkflowValidatorByStep(List<String> steps) {
        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();
        Set<String> invalidSteps = new HashSet<>(steps);

        for (WorkflowSteps mapping : WorkflowSteps.values()) {
            String step = mapping.getWorkflowStepName();
            if (steps.contains(step)) {
                workflowStepValidators.put(mapping.getWorkflowStepName(), mapping.getWorkflowStepValidator());
                invalidSteps.remove(step);
            }
        }

        if (!invalidSteps.isEmpty()) {
            throw new FlowFrameworkException("Invalid step name: " + invalidSteps, RestStatus.BAD_REQUEST);
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
