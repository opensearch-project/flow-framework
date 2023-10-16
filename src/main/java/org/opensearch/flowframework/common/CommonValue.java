/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

/**
 * Representation of common values that are used across project
 */
public class CommonValue {

    private CommonValue() {}

    /** Default value for no schema version */
    public static Integer NO_SCHEMA_VERSION = 0;
    /** Index mapping meta field name*/
    public static final String META = "_meta";
    /** Schema Version field name */
    public static final String SCHEMA_VERSION_FIELD = "schema_version";
    /** Global Context Index Name */
    public static final String GLOBAL_CONTEXT_INDEX = ".plugins-ai-global-context";
    /** Global Context index mapping file path */
    public static final String GLOBAL_CONTEXT_INDEX_MAPPING = "mappings/global-context.json";
    /** Global Context index mapping version */
    public static final Integer GLOBAL_CONTEXT_INDEX_VERSION = 1;

    /** The transport action name prefix */
    public static final String TRANSPORT_ACION_NAME_PREFIX = "cluster:admin/opensearch/flow_framework/";
    /** The base URI for this plugin's rest actions */
    public static final String FLOW_FRAMEWORK_BASE_URI = "/_plugins/_flow_framework";
    /** The URI for this plugin's workflow rest actions */
    public static final String WORKFLOW_URI = FLOW_FRAMEWORK_BASE_URI + "/workflow";
    /** Field name for workflow Id, the document Id of the indexed use case template */
    public static final String WORKFLOW_ID = "workflow_id";
    /** The field name for provision workflow within a use case template*/
    public static final String PROVISION_WORKFLOW = "provision";

    /** Flow Framework plugin thread pool name prefix */
    public static final String FLOW_FRAMEWORK_THREAD_POOL_PREFIX = "thread_pool.flow_framework.";
    /** The provision workflow thread pool name */
    public static final String PROVISION_THREAD_POOL = "opensearch_workflow_provision";

    /** Model Id field */
    public static final String MODEL_ID = "model_id";
    /** Function Name field */
    public static final String FUNCTION_NAME = "function_name";
    /** Model Name field */
    public static final String MODEL_NAME = "name";
    /** Model Version field */
    public static final String MODEL_VERSION = "model_version";
    /** Model Group Id field */
    public static final String MODEL_GROUP_ID = "model_group_id";
    /** Description field */
    public static final String DESCRIPTION = "description";
    /** Connector Id field */
    public static final String CONNECTOR_ID = "connector_id";
    /** Model format field */
    public static final String MODEL_FORMAT = "model_format";
    /** Model config field */
    public static final String MODEL_CONFIG = "model_config";
}
