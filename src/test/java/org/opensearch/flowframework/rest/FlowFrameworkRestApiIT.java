/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;

import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkRestApiIT extends FlowFrameworkRestTestCase {

    public void testCreateWorkflow() throws Exception {

        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        // Hit Create Workflow API
        Response response = createWorkflow(template);
        assertEquals(RestStatus.CREATED.getStatus(), response.getStatusLine().getStatusCode());

        // Hit Provision API
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        response = provisionWorkflow(workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

}
