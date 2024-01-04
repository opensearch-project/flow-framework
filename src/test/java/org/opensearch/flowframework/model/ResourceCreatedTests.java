/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.flowframework.common.WorkflowResources.CREATE_CONNECTOR;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

public class ResourceCreatedTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testParseFeature() throws IOException {
        String workflowStepName = CREATE_CONNECTOR.getWorkflowStep();
        String resourceType = getResourceByWorkflowStep(workflowStepName);
        ResourceCreated resourceCreated = new ResourceCreated(workflowStepName, "workflow_step_1", resourceType, "L85p1IsBbfF");
        assertEquals(workflowStepName, resourceCreated.workflowStepName());
        assertEquals("workflow_step_1", resourceCreated.workflowStepId());
        assertEquals("connector_id", resourceCreated.resourceType());
        assertEquals("L85p1IsBbfF", resourceCreated.resourceId());

        String json = TemplateTestJsonUtil.parseToJson(resourceCreated);
        assertTrue(json.contains("\"workflow_step_name\":\"create_connector\""));
        assertTrue(json.contains("\"workflow_step_id\":\"workflow_step_1\""));
        assertTrue(json.contains("\"resource_type\":\"connector_id\""));
        assertTrue(json.contains("\"resource_id\":\"L85p1IsBbfF\""));

        ResourceCreated resourceCreatedTwo = ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals(workflowStepName, resourceCreatedTwo.workflowStepName());
        assertEquals("workflow_step_1", resourceCreatedTwo.workflowStepId());
        assertEquals("L85p1IsBbfF", resourceCreatedTwo.resourceId());
    }
}
