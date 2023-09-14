/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.util.HashMap;
import java.util.Map;

public class CreateIngestPipelineResponseData implements WorkflowData {

    private Map<String, Object> content = new HashMap<>();

    public CreateIngestPipelineResponseData(String ingestPipelineId) {
        super();
        // PutPipelineAction returns only an acknodledged response, returning ingest pipeline id instead
        content.put("pipelineId", ingestPipelineId);
    }

    @Override
    public Map<String, Object> getContent() {
        return Map.copyOf(content);
    }
}
