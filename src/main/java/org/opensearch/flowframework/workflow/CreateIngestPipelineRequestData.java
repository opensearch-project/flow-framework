/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.ingest.PutPipelineRequest;

import java.util.HashMap;
import java.util.Map;

public class CreateIngestPipelineRequestData implements WorkflowInputData {

    private Map<String, String> params = new HashMap<>();
    private Map<String, Object> content = new HashMap<>();

    public CreateIngestPipelineRequestData(PutPipelineRequest request) {
        super();

        // RestPutPipelineAction params
        params.put("id", request.getId());

        // PutPipelineRequest content
        content.put("source", request.getSource());
        content.put("mediaType", request.getMediaType());
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
