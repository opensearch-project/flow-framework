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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum encapsulating the different step names and the resources they create
 */
public enum WorkflowResources {

    /** official workflow step name for creating a connector and associated created resource */
    CREATE_CONNECTOR("create_connector", "connector_id"),
    /** official workflow step name for registering a remote model and associated created resource */
    REGISTER_REMOTE_MODEL("register_remote_model", "model_id"),
    /** official workflow step name for registering a local model and associated created resource */
    REGISTER_LOCAL_MODEL("register_local_model", "model_id"),
    /** official workflow step name for registering a model group and associated created resource */
    REGISTER_MODEL_GROUP("register_model_group", "model_group_id"),
    /** official workflow step name for deploying a model and associated created resource */
    DEPLOY_MODEL("deploy_model", "model_id"),
    /** official workflow step name for creating an ingest-pipeline and associated created resource */
    CREATE_INGEST_PIPELINE("create_ingest_pipeline", "pipeline_id"),
    /** official workflow step name for creating an index and associated created resource */
    CREATE_INDEX("create_index", "index_name");

    private final String workflowStep;
    private final String resourceCreated;
    private static final Logger logger = LogManager.getLogger(WorkflowResources.class);
    private static final Set<String> allResources = Stream.of(values())
        .map(WorkflowResources::getResourceCreated)
        .collect(Collectors.toSet());

    WorkflowResources(String workflowStep, String resourceCreated) {
        this.workflowStep = workflowStep;
        this.resourceCreated = resourceCreated;
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
     * gets the resources created type based on the workflowStep
     * @param workflowStep workflow step name
     * @return the resource that will be created
     * @throws FlowFrameworkException if workflow step doesn't exist in enum
     */
    public static String getResourceByWorkflowStep(String workflowStep) throws FlowFrameworkException {
        if (workflowStep != null && !workflowStep.isEmpty()) {
            for (WorkflowResources mapping : values()) {
                if (mapping.getWorkflowStep().equals(workflowStep)) {
                    return mapping.getResourceCreated();
                }
            }
        }
        logger.error("Unable to find resource type for step: " + workflowStep);
        throw new FlowFrameworkException("Unable to find resource type for step: " + workflowStep, RestStatus.BAD_REQUEST);
    }

    /**
     * Returns all the possible resource created types in enum
     * @return a set of all the resource created types
     */
    public static Set<String> getAllResourcesCreated() {
        return allResources;
    }
}
