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
    /** Workflow State Index Name */
    public static final String WORKFLOW_STATE_INDEX = ".plugins-workflow-state";
    /** Workflow State index mapping file path */
    public static final String WORKFLOW_STATE_INDEX_MAPPING = "mappings/workflow-state.json";
    /** Workflow State index mapping version */
    public static final Integer WORKFLOW_STATE_INDEX_VERSION = 1;

    /** The template field name for template use case */
    public static final String USE_CASE_FIELD = "use_case";
    /** The template field name for template version */
    public static final String TEMPLATE_FIELD = "template";
    /** The template field name for template compatibility with OpenSearch versions */
    public static final String COMPATIBILITY_FIELD = "compatibility";
    /** The template field name for template workflows */
    public static final String WORKFLOWS_FIELD = "workflows";
    /** The template field name for the user who created the workflow **/
    public static final String USER_FIELD = "user";

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

    /** Index name field */
    public static final String INDEX_NAME = "index_name";
    /** Type field */
    public static final String TYPE = "type";
    /** ID Field */
    public static final String ID = "id";
    /** Pipeline Id field */
    public static final String PIPELINE_ID = "pipeline_id";
    /** Processors field */
    public static final String PROCESSORS = "processors";
    /** Field map field */
    public static final String FIELD_MAP = "field_map";
    /** Input Field Name field */
    public static final String INPUT_FIELD_NAME = "input_field_name";
    /** Output Field Name field */
    public static final String OUTPUT_FIELD_NAME = "output_field_name";
    /** Model Id field */
    public static final String MODEL_ID = "model_id";
    /** Function Name field */
    public static final String FUNCTION_NAME = "function_name";
    /** Name field */
    public static final String NAME_FIELD = "name";
    /** Model Version field */
    public static final String MODEL_VERSION = "model_version";
    /** Model Group Id field */
    public static final String MODEL_GROUP_ID = "model_group_id";
    /** Description field */
    public static final String DESCRIPTION_FIELD = "description";
    /** Connector Id field */
    public static final String CONNECTOR_ID = "connector_id";
    /** Model format field */
    public static final String MODEL_FORMAT = "model_format";
    /** Model config field */
    public static final String MODEL_CONFIG = "model_config";
    /** Version field */
    public static final String VERSION_FIELD = "version";
    /** Connector protocol field */
    public static final String PROTOCOL_FIELD = "protocol";
    /** Connector parameters field */
    public static final String PARAMETERS_FIELD = "parameters";
    /** Connector credentials field */
    public static final String CREDENTIALS_FIELD = "credentials";
    /** Connector actions field */
    public static final String ACTIONS_FIELD = "actions";
    /** Backend roles for the model */
    public static final String BACKEND_ROLES_FIELD = "backend_roles";
    /** Access mode for the model */
    public static final String MODEL_ACCESS_MODE = "access_mode";
    /** Add all backend roles */
    public static final String ADD_ALL_BACKEND_ROLES = "add_all_backend_roles";

    /** The template field name for the associated workflowID **/
    public static final String WORKFLOW_ID_FIELD = "workflow_id";
    /** The template field name for the workflow error **/
    public static final String ERROR_FIELD = "error";
    /** The template field name for the workflow state **/
    public static final String STATE_FIELD = "state";
    /** The template field name for the workflow provisioning progress **/
    public static final String PROVISIONING_PROGRESS_FIELD = "provisioning_progress";
    /** The template field name for the workflow provisioning start time **/
    public static final String PROVISION_START_TIME_FIELD = "provision_start_time";
    /** The template field name for the workflow provisioning end time **/
    public static final String PROVISION_END_TIME_FIELD = "provision_end_time";
    /** The template field name for the workflow ui metadata **/
    public static final String UI_METADATA_FIELD = "ui_metadata";
    /** The template field name for template user outputs */
    public static final String USER_OUTPUTS_FIELD = "user_outputs";
    /** The template field name for template resources created */
    public static final String RESOURCES_CREATED_FIELD = "resources_created";
}
