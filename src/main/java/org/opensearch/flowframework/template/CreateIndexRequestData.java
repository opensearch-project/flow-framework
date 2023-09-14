/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.flowframework.workflow.WorkflowInputData;

import java.util.HashMap;
import java.util.Map;

public class CreateIndexRequestData implements WorkflowInputData {

    private Map<String, String> params = new HashMap<>();
    private Map<String, Object> content = new HashMap<>();

    public CreateIndexRequestData(CreateIndexRequest request) {
        super();
        // See RestCreateIndexAction for source of param keys needed
        params.put("index", request.index());
        // See CreateIndexRequest ParseFields for source of content keys needed
        content.put("mappings", request.mappings());
        content.put("settings", request.settings());
        content.put("aliases", request.aliases());
    }

    @Override
    public Map<String, Object> getContent() {
        return Map.copyOf(content);
    }

    @Override
    public Map<String, String> getParams() {
        return Map.copyOf(params);
    }

}
