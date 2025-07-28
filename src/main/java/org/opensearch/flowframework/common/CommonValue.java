/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.Version;
import org.opensearch.common.xcontent.XContentContraints;

/**
 * Representation of common values that are used across project
 */
public class CommonValue {

    private CommonValue() {}

    /*
     * Constants associated with Global Context
     */
    /** Default value for no schema version */
    public static final Integer NO_SCHEMA_VERSION = 0;
    /** Index mapping meta field name*/
    public static final String META = "_meta";
    /** Schema Version field name */
    public static final String SCHEMA_VERSION_FIELD = "schema_version";
    /** Global Context Index Name */
    public static final String GLOBAL_CONTEXT_INDEX = ".plugins-flow-framework-templates";
    /** Global Context index mapping file path */
    public static final String GLOBAL_CONTEXT_INDEX_MAPPING = "mappings/global-context.json";
    /** Global Context index mapping version */
    public static final Integer GLOBAL_CONTEXT_INDEX_VERSION = 1;
    /** Workflow State Index Name */
    public static final String WORKFLOW_STATE_INDEX = ".plugins-flow-framework-state";
    /** Workflow State index mapping file path */
    public static final String WORKFLOW_STATE_INDEX_MAPPING = "mappings/workflow-state.json";
    /** Workflow State index mapping version */
    public static final Integer WORKFLOW_STATE_INDEX_VERSION = 1;
    /** Config Index Name */
    public static final String CONFIG_INDEX = ".plugins-flow-framework-config";
    /** Config index mapping file path */
    public static final String CONFIG_INDEX_MAPPING = "mappings/config.json";
    /** Config index mapping version */
    public static final Integer CONFIG_INDEX_VERSION = 1;
    /** Master key field name */
    public static final String MASTER_KEY = "master_key";
    /** Create Time field  name */
    public static final String CREATE_TIME = "create_time";
    /** The template field name for the user who created the workflow **/
    public static final String USER_FIELD = "user";
    /** The created time field */
    public static final String CREATED_TIME = "created_time";
    /** The last updated time field */
    public static final String LAST_UPDATED_TIME_FIELD = "last_updated_time";
    /** The last provisioned time field */
    public static final String LAST_PROVISIONED_TIME_FIELD = "last_provisioned_time";
    /** Maximum allowed JSON size in bytes (~50MB). */
    public static final int MAX_JSON_SIZE = XContentContraints.DEFAULT_MAX_STRING_LEN; // 50000000
    /** Maximum allowed name length for JSON keys(50000b). */
    public static final int MAX_JSON_NAME_LENGTH = XContentContraints.DEFAULT_MAX_NAME_LEN; // 50000
    /** Maximum allowed nesting depth for JSON structures. */
    public static final int MAX_JSON_NESTING_DEPTH = XContentContraints.DEFAULT_MAX_DEPTH; // 1000

    /*
     * Constants associated with Rest or Transport actions
     */
    /** The transport action name prefix */
    public static final String TRANSPORT_ACTION_NAME_PREFIX = "cluster:admin/opensearch/flow_framework/";
    /** The base URI for this plugin's rest actions */
    public static final String FLOW_FRAMEWORK_BASE_URI = "/_plugins/_flow_framework";
    /** The URI for this plugin's workflow rest actions */
    public static final String WORKFLOW_URI = FLOW_FRAMEWORK_BASE_URI + "/workflow";
    /** Field name for workflow Id, the document Id of the indexed use case template */
    public static final String WORKFLOW_ID = "workflow_id";
    /** Field name for template validation, the flag to indicate if validation is necessary */
    public static final String VALIDATION = "validation";
    /** Param name for allow deletion during deprovisioning */
    public static final String ALLOW_DELETE = "allow_delete";
    /** The field name for provision workflow within a use case template*/
    public static final String PROVISION_WORKFLOW = "provision";
    /** The param name for update workflow field in create API */
    public static final String UPDATE_WORKFLOW_FIELDS = "update_fields";
    /** The param name for specifying the timeout duration in seconds to wait for workflow completion */
    public static final String WAIT_FOR_COMPLETION_TIMEOUT = "wait_for_completion_timeout";
    /** The field name for workflow steps. This field represents the name of the workflow steps to be fetched. */
    public static final String WORKFLOW_STEP = "workflow_step";
    /** The param name for default use case, used by the create workflow API */
    public static final String USE_CASE = "use_case";
    /** The param name for reprovisioning, used by the create workflow API */
    public static final String REPROVISION_WORKFLOW = "reprovision";
    /** The REST header containing the tenant id */
    public static final String TENANT_ID_HEADER = "x-tenant-id";
    /** The field name containing the tenant id */
    public static final String TENANT_ID_FIELD = "tenant_id";

    /*
     * Constants associated with plugin configuration
     */
    /** Flow Framework plugin thread pool name prefix */
    public static final String FLOW_FRAMEWORK_THREAD_POOL_PREFIX = "thread_pool.flow_framework.";
    /** The general workflow thread pool name for most calls */
    public static final String WORKFLOW_THREAD_POOL = "opensearch_workflow";
    /** The workflow thread pool name for provisioning */
    public static final String PROVISION_WORKFLOW_THREAD_POOL = "opensearch_provision_workflow";
    /** The workflow thread pool name for deprovisioning */
    public static final String DEPROVISION_WORKFLOW_THREAD_POOL = "opensearch_deprovision_workflow";

    /*
     * Field names common to multiple classes
     */
    /** Success name field */
    public static final String SUCCESS = "success";
    /** Type field */
    public static final String TYPE = "type";
    /** default_mapping_option filed */
    public static final String DEFAULT_MAPPING_OPTION = "default_mapping_option";
    /** ID Field */
    public static final String ID = "id";
    /** Processors field */
    public static final String PROCESSORS = "processors";
    /** Field map field */
    public static final String FIELD_MAP = "field_map";
    /** Input Field Name field */
    public static final String INPUT_FIELD_NAME = "input_field_name";
    /** Output Field Name field */
    public static final String OUTPUT_FIELD_NAME = "output_field_name";
    /** Task Id field */
    public static final String TASK_ID = "task_id";
    /** Register Model Status field */
    public static final String REGISTER_MODEL_STATUS = "register_model_status";
    /** Function Name field */
    public static final String FUNCTION_NAME = "function_name";
    /** Name field */
    public static final String NAME_FIELD = "name";
    /** Model Version field */
    public static final String MODEL_VERSION = "model_version";
    /** Model Group Id field */
    public static final String MODEL_GROUP_STATUS = "model_group_status";
    /** Description field */
    public static final String DESCRIPTION_FIELD = "description";
    /** Description field */
    public static final String DEPLOY_FIELD = "deploy";
    /** Model format field */
    public static final String MODEL_FORMAT = "model_format";
    /** Model content hash value field */
    public static final String MODEL_CONTENT_HASH_VALUE = "model_content_hash_value";
    /** URL field */
    public static final String URL = "url";
    /** Model config field */
    public static final String MODEL_CONFIG = "model_config";
    /** Additional config field */
    public static final String ADDITIONAL_CONFIG = "additional_config";
    /** Space type field */
    public static final String SPACE_TYPE = "space_type";
    /** Model type field */
    public static final String MODEL_TYPE = "model_type";
    /** Embedding dimension field */
    public static final String EMBEDDING_DIMENSION = "embedding_dimension";
    /** Framework type field */
    public static final String FRAMEWORK_TYPE = "framework_type";
    /** All config field */
    public static final String ALL_CONFIG = "all_config";
    /** Version field */
    public static final String VERSION_FIELD = "version";
    /** Connector protocol field */
    public static final String PROTOCOL_FIELD = "protocol";
    /** Connector parameters field */
    public static final String PARAMETERS_FIELD = "parameters";
    /** Connector credential field */
    public static final String CREDENTIAL_FIELD = "credential";
    /** Connector actions field */
    public static final String ACTIONS_FIELD = "actions";
    /** Backend roles for the model */
    public static final String BACKEND_ROLES_FIELD = "backend_roles";
    /** Access mode for the model */
    public static final String MODEL_ACCESS_MODE = "access_mode";
    /** Add all backend roles */
    public static final String ADD_ALL_BACKEND_ROLES = "add_all_backend_roles";
    /** The tools field for an agent */
    public static final String TOOLS_FIELD = "tools";
    /** The tools order field for an agent */
    public static final String TOOLS_ORDER_FIELD = "tools_order";
    /** The memory field for an agent */
    public static final String MEMORY_FIELD = "memory";
    /** The app type field for an agent */
    public static final String APP_TYPE_FIELD = "app_type";
    /** Pipeline ID, also corresponds to pipeline name */
    public static final String PIPELINE_ID = "pipeline_id";
    /** Pipeline Configurations */
    public static final String CONFIGURATIONS = "configurations";
    /** The llm field */
    public static final String LLM = "llm";
    /** Guardrails field */
    public static final String GUARDRAILS_FIELD = "guardrails";
    /** Delay field */
    public static final String DELAY_FIELD = "delay";
    /** Model Interface Field */
    public static final String INTERFACE_FIELD = "interface";
    /** The source index field for reindex */
    public static final String SOURCE_INDEX = "source_index";
    /** The destination index field for reindex */
    public static final String DESTINATION_INDEX = "destination_index";
    /** Provision Timeout field */
    public static final String PROVISION_TIMEOUT_FIELD = "provision.timeout";
    /*
     * Constants associated with resource provisioning / state
     */
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
    /** The parameter to clear workflow state when deleting template */
    public static final String CLEAR_STATUS = "clear_status";
    /** The field name for the step name where a resource is created */
    public static final String WORKFLOW_STEP_NAME = "workflow_step_name";
    /** The field name for the step ID where a resource is created */
    public static final String WORKFLOW_STEP_ID = "workflow_step_id";
    /** The field name for the resource type */
    public static final String RESOURCE_TYPE = "resource_type";
    /** The field name for the resource id */
    public static final String RESOURCE_ID = "resource_id";
    /** The field name for the opensearch-ml plugin */
    public static final String OPENSEARCH_ML = "opensearch-ml";

    /*
     * Constants assoicated with substitution / default templates
     */
    /** The field name for connector credential key substitution */
    public static final String CREATE_CONNECTOR_CREDENTIAL_KEY = "create_connector.credential.key";
    /** The field name for connector credential access key substitution */
    public static final String CREATE_CONNECTOR_CREDENTIAL_ACCESS_KEY = "create_connector.credential.access_key";
    /** The field name for connector credential secret key substitution */
    public static final String CREATE_CONNECTOR_CREDENTIAL_SECRET_KEY = "create_connector.credential.secret_key";
    /** The field name for connector credential session token substitution */
    public static final String CREATE_CONNECTOR_CREDENTIAL_SESSION_TOKEN = "create_connector.credential.session_token";
    /** The field name for ingest pipeline model ID substitution */
    public static final String CREATE_INGEST_PIPELINE_MODEL_ID = "create_ingest_pipeline.model_id";
    /** The field name for reindex source index substitution */
    public static final String REINDEX_SOURCE_INDEX = "reindex.source_index";

    /**URI for the YAML file of the ML Commons API specification.*/
    public static final String ML_COMMONS_API_SPEC_YAML_URI =
        "https://raw.githubusercontent.com/opensearch-project/opensearch-api-specification/refs/heads/main/spec/namespaces/ml.yaml";

    /*
     * Constants associated with non-BWC features
     */
    /** Version 2.19.0 */
    public static final Version VERSION_2_19_0 = Version.fromString("2.19.0");
}
