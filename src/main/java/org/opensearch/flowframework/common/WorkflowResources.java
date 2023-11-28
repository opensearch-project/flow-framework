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

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enum encapsulating the different step names and the resources they create
 */
public enum WorkflowResources {

    CREATE_CONNECTOR("create_connector", "connector_id"),
    REGISTER_REMOTE_MODEL("register_remote_model", "model_id"),
    REGISTER_LOCAL_MODEL("register_local_model", "task_id"),
    MODEL_GROUP("register_model_group", "group_model_id"),
    CREATE_INGEST_PIPELINE("create_ingest_pipeline", "pipeline_id"),
    CREATE_INDEX("create_index", "index");

    private final String workflowStep;
    private final String resourceCreated;
    private static final Logger logger = LogManager.getLogger(WorkflowResources.class);

    WorkflowResources(String workflowStep, String resourceCreated) {
        this.workflowStep = workflowStep;
        this.resourceCreated = resourceCreated;
    }

    public String getWorkflowStep() {
        return workflowStep;
    }

    public String getResourceCreated() {
        return resourceCreated;
    }

    /**
     * gets the resources created type based on the workflowStep
     * @param workflowStep workflow step name
     * @return the resource that will be created
     * @throws IOException if workflow step doesn't exist in enum
     */
    public static String getResourceByWorkflowStep(String workflowStep) throws IOException {
        if (workflowStep != null && !workflowStep.isEmpty()) {
            for (WorkflowResources mapping : values()) {
                if (mapping.getWorkflowStep().equals(workflowStep)) {
                    return mapping.getResourceCreated();
                }
            }
        }
        logger.error("Unable to find resource type for step: " + workflowStep);
        throw new IOException("Unable to find resource type for step: " + workflowStep);
    }

    public static Set<String> getAllKeys() {
        return Stream.of(values()).map(WorkflowResources::getResourceCreated).collect(Collectors.toSet());
    }
}
