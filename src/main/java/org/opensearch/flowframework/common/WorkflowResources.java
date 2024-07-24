/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.workflow.CreateConnectorStep;
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.flowframework.workflow.CreateIngestPipelineStep;
import org.opensearch.flowframework.workflow.CreateSearchPipelineStep;
import org.opensearch.flowframework.workflow.DeleteAgentStep;
import org.opensearch.flowframework.workflow.DeleteConnectorStep;
import org.opensearch.flowframework.workflow.DeleteIngestPipelineStep;
import org.opensearch.flowframework.workflow.DeleteModelStep;
import org.opensearch.flowframework.workflow.DeleteSearchPipelineStep;
import org.opensearch.flowframework.workflow.DeployModelStep;
import org.opensearch.flowframework.workflow.NoOpStep;
import org.opensearch.flowframework.workflow.RegisterAgentStep;
import org.opensearch.flowframework.workflow.RegisterLocalCustomModelStep;
import org.opensearch.flowframework.workflow.RegisterLocalPretrainedModelStep;
import org.opensearch.flowframework.workflow.RegisterLocalSparseEncodingModelStep;
import org.opensearch.flowframework.workflow.RegisterModelGroupStep;
import org.opensearch.flowframework.workflow.RegisterRemoteModelStep;
import org.opensearch.flowframework.workflow.ReindexStep;
import org.opensearch.flowframework.workflow.UndeployModelStep;
import org.opensearch.flowframework.workflow.UpdateIndexStep;
import org.opensearch.flowframework.workflow.UpdateIngestPipelineStep;
import org.opensearch.flowframework.workflow.UpdateSearchPipelineStep;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum encapsulating the different step names and the resources they create
 */
public enum WorkflowResources {

    /** Workflow steps for creating/deleting a connector and associated created resource */
    CREATE_CONNECTOR(CreateConnectorStep.NAME, WorkflowResources.CONNECTOR_ID, DeleteConnectorStep.NAME, null),
    /** Workflow steps for registering/deleting a remote model and associated created resource */
    REGISTER_REMOTE_MODEL(RegisterRemoteModelStep.NAME, WorkflowResources.MODEL_ID, DeleteModelStep.NAME, null),
    /** Workflow steps for registering/deleting a local model and associated created resource */
    REGISTER_LOCAL_MODEL(RegisterLocalCustomModelStep.NAME, WorkflowResources.MODEL_ID, DeleteModelStep.NAME, null),
    /** Workflow steps for registering/deleting a local sparse encoding model and associated created resource */
    REGISTER_LOCAL_SPARSE_ENCODING_MODEL(RegisterLocalSparseEncodingModelStep.NAME, WorkflowResources.MODEL_ID, DeleteModelStep.NAME, null),
    /** Workflow steps for registering/deleting a local OpenSearch provided pretrained model and associated created resource */
    REGISTER_LOCAL_PRETRAINED_MODEL(RegisterLocalPretrainedModelStep.NAME, WorkflowResources.MODEL_ID, DeleteModelStep.NAME, null),
    /** Workflow steps for registering/deleting a model group and associated created resource */
    REGISTER_MODEL_GROUP(RegisterModelGroupStep.NAME, WorkflowResources.MODEL_GROUP_ID, NoOpStep.NAME, null),
    /** Workflow steps for deploying/undeploying a model and associated created resource */
    DEPLOY_MODEL(DeployModelStep.NAME, WorkflowResources.MODEL_ID, UndeployModelStep.NAME, null),
    /** Workflow steps for creating an ingest-pipeline and associated created resource */
    CREATE_INGEST_PIPELINE(
        CreateIngestPipelineStep.NAME,
        WorkflowResources.PIPELINE_ID,
        DeleteIngestPipelineStep.NAME,
        UpdateIngestPipelineStep.NAME
    ),
    /** Workflow steps for creating an ingest-pipeline and associated created resource */
    CREATE_SEARCH_PIPELINE(
        CreateSearchPipelineStep.NAME,
        WorkflowResources.PIPELINE_ID,
        DeleteSearchPipelineStep.NAME,
        UpdateSearchPipelineStep.NAME
    ),
    /** Workflow steps for creating an index and associated created resource */
    CREATE_INDEX(CreateIndexStep.NAME, WorkflowResources.INDEX_NAME, NoOpStep.NAME, UpdateIndexStep.NAME),
    /** Workflow steps for reindex a source index to destination index and associated created resource */
    REINDEX(ReindexStep.NAME, WorkflowResources.INDEX_NAME, NoOpStep.NAME, null),
    /** Workflow steps for registering/deleting an agent and the associated created resource */
    REGISTER_AGENT(RegisterAgentStep.NAME, WorkflowResources.AGENT_ID, DeleteAgentStep.NAME, null);

    /** Connector Id for a remote model connector */
    public static final String CONNECTOR_ID = "connector_id";
    /** Model Id for an ML model */
    public static final String MODEL_ID = "model_id";
    /** Model Group Id */
    public static final String MODEL_GROUP_ID = "model_group_id";
    /** Pipeline Id for Ingest Pipeline */
    public static final String PIPELINE_ID = "pipeline_id";
    /** Index name */
    public static final String INDEX_NAME = "index_name";
    /** Agent Id */
    public static final String AGENT_ID = "agent_id";

    private final String workflowStep;
    private final String resourceCreated;
    private final String deprovisionStep;
    private final String updateStep;
    private static final Logger logger = LogManager.getLogger(WorkflowResources.class);
    private static final Set<String> allResources = Stream.of(values())
        .map(WorkflowResources::getResourceCreated)
        .collect(Collectors.toSet());

    WorkflowResources(String workflowStep, String resourceCreated, String deprovisionStep, String updateStep) {
        this.workflowStep = workflowStep;
        this.resourceCreated = resourceCreated;
        this.deprovisionStep = deprovisionStep;
        this.updateStep = updateStep;
    }

    /**
     * Returns the workflowStep for the given enum Constant
     * @return the workflowStep of this data.
     */
    public String getWorkflowStep() {
        return workflowStep;
    }

    /**
     * Returns the resourceCreated for the given enum Constant
     * @return the resourceCreated of this data.
     */
    public String getResourceCreated() {
        return resourceCreated;
    }

    /**
     * Returns the deprovisionStep for the given enum Constant
     * @return the deprovisionStep of this data.
     */
    public String getDeprovisionStep() {
        return deprovisionStep;
    }

    /**
     * Returns the updateStep for the given enum Constant
     * @return the updateStep of this data.
     */
    public String getUpdateStep() {
        return updateStep;
    }

    /**
     * Gets the resources created type based on the workflowStep.
     * @param workflowStep workflow step name
     * @return the resource that will be created
     * @throws FlowFrameworkException if workflow step doesn't exist in enum
     */
    public static String getResourceByWorkflowStep(String workflowStep) throws FlowFrameworkException {
        if (workflowStep != null && !workflowStep.isEmpty()) {
            for (WorkflowResources mapping : values()) {
                if (workflowStep.equals(mapping.getWorkflowStep())
                    || workflowStep.equals(mapping.getDeprovisionStep())
                    || workflowStep.equals(mapping.getUpdateStep())) {
                    return mapping.getResourceCreated();
                }
            }
        }
        logger.error("Unable to find resource type for step: {}", workflowStep);
        throw new FlowFrameworkException("Unable to find resource type for step: " + workflowStep, RestStatus.BAD_REQUEST);
    }

    /**
     * Gets the deprovision step type based on the workflowStep.
     * @param workflowStep workflow step name
     * @return the corresponding step to deprovision
     * @throws FlowFrameworkException if workflow step doesn't exist in enum
     */
    public static String getDeprovisionStepByWorkflowStep(String workflowStep) throws FlowFrameworkException {
        if (workflowStep != null && !workflowStep.isEmpty()) {
            for (WorkflowResources mapping : values()) {
                if (mapping.getWorkflowStep().equals(workflowStep)) {
                    return mapping.getDeprovisionStep();
                }
            }
        }
        logger.error("Unable to find deprovision step for step: {}", workflowStep);
        throw new FlowFrameworkException("Unable to find deprovision step for step: " + workflowStep, RestStatus.BAD_REQUEST);
    }

    /**
     * Gets the update step type based on the workflowStep.
     * @param workflowStep workflow step name
     * @return the corresponding step to update
     * @throws FlowFrameworkException if workflow step doesn't exist in enum
     */
    public static String getUpdateStepByWorkflowStep(String workflowStep) throws FlowFrameworkException {
        if (workflowStep != null && !workflowStep.isEmpty()) {
            for (WorkflowResources mapping : values()) {
                if (mapping.getWorkflowStep().equals(workflowStep)) {
                    return mapping.getUpdateStep();
                }
            }
        }
        logger.error("Unable to find update step for step: {}", workflowStep);
        throw new FlowFrameworkException("Unable to find update step for step: " + workflowStep, RestStatus.BAD_REQUEST);
    }

    /**
     * Returns all the possible resource created types in enum
     * @return a set of all the resource created types
     */
    public static Set<String> getAllResourcesCreated() {
        return allResources;
    }
}
