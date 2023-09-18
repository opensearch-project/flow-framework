/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.constant;

public class CommonValue {

    public static Integer NO_SCHEMA_VERSION = 0;
    public static final String META = "_meta";
    public static final String SCHEMA_VERSION_FIELD = "schema_version";
    public static final Integer GLOBAL_CONTEXT_INDEX_SCHEMA_VERSION = 1;
    public static final String GLOBAL_CONTEXT_INDEX_MAPPING =
            "{\n  " +
            "   \"dynamic\": false,\n" +
            "   \"_meta\": {\n" +
            "       \"schema_version\": 1\n" +
            "   },\n" +
            "   \"properties\": {\n" +
            "       \"pipeline_id\": {\n" +
            "           \"type\": \"keyword\"\n"+
            "       },\n" +
            "       \"name\": {\n" +
            "           \"type\": \"text\",\n" +
            "           \"fields\": {\n" +
            "               \"keyword\": {\n" +
            "                   \"type\": \"keyword\",\n" +
            "                   \"ignore_above\": 256\n" +
            "               }\n" +
            "           }\n" +
            "       },\n" +
            "       \"description\": {\n" +
            "           \"type\": \"text\"\n" +
            "       },\n" +
            "       \"use_case\": {\n" +
            "           \"type\": \"keyword\"\n" +
            "       },\n" +
            "       \"operations\": {\n" +
            "           \"type\": \"keyword\"\n" +
            "       },\n" +
            "       \"version\": {\n" +
            "           \"type\": \"nested\",\n" +
            "           \"properties\": {\n" +
            "               \"template\": {\n" +
            "                   \"type\": \"integer\"\n" +
            "               },\n" +
            "               \"compatibility\": {\n" +
            "                   \"type\": \"integer\"\n" +
            "               }\n" +
            "           }\n" +
            "       },\n" +
            "       \"user_inputs\": {\n" +
            "           \"type\": \"nested\",\n" +
            "           \"properties\": {\n" +
            "               \"model_id\": {\n" +
            "                   \"type\": \"keyword\"\n" +
            "               },\n" +
            "               \"input_field\": {\n" +
            "                   \"type\": \"keyword\"\n" +
            "               },\n" +
            "               \"output_field\": {\n" +
            "                   \"type\": \"keyword\"\n" +
            "               },\n" +
            "               \"ingest_pipeline_name\": {\n" +
            "                   \"type\": \"keyword\"\n" +
            "               },\n" +
            "               \"index_name\": {\n" +
            "                   \"type\": \"keyword\"\n" +
            "               }\n" +
            "           }\n" +
            "       },\n" +
            "       \"workflows\": {\n" +
            "           \"type\": \"text\"\n" +
            "       },\n" +
            "       \"responses\": {\n" +
            "           \"type\": \"text\"\n" +
            "       }\n" +
            "       \"resources_created\": {\n" +
            "           \"type\": \"text\"\n" +
            "       }\n" +
            "   }\n" +
            "}";
}
