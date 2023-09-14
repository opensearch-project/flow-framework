/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.flowframework.workflow.WorkflowData;

import java.util.HashMap;
import java.util.Map;

public class CreateIndexResponseData implements WorkflowData {

    private Map<String, Object> content = new HashMap<>();

    public CreateIndexResponseData(CreateIndexResponse response) {
        super();
        // See CreateIndexResponse ParseFields for source of content keys needed
        content.put("index", response.index());
    }

    @Override
    public Map<String, Object> getContent() {
        return Map.copyOf(content);
    }
}
