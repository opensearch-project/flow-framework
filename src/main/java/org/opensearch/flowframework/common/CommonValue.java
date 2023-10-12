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

    public static Integer NO_SCHEMA_VERSION = 0;
    public static final String META = "_meta";
    public static final String SCHEMA_VERSION_FIELD = "schema_version";
    public static final String GLOBAL_CONTEXT_INDEX = ".plugins-ai-global-context";
    public static final String GLOBAL_CONTEXT_INDEX_MAPPING = "mappings/global-context.json";
    public static final Integer GLOBAL_CONTEXT_INDEX_VERSION = 1;
    public static final String MODEL_ID = "model_id";
    public static final String FUNCTION_NAME = "function_name";
    public static final String MODEL_NAME = "name";
    public static final String MODEL_VERSION = "model_version";
    public static final String MODEL_GROUP_ID = "model_group_id";
    public static final String DESCRIPTION = "description";
    public static final String CONNECTOR_ID = "connector_id";
    public static final String MODEL_FORMAT = "model_format";
    public static final String MODEL_CONFIG = "model_config";
}
