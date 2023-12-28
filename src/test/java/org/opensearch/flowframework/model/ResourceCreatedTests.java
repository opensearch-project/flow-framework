/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
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
        assertEquals(CONNECTOR_ID, resourceCreated.resourceType());
        assertEquals("L85p1IsBbfF", resourceCreated.resourceId());

        String expectedJson =
            "{\"workflow_step_name\":\"create_connector\",\"workflow_step_id\":\"workflow_step_1\",\"resource_type\":\"connector_id\",\"resource_id\":\"L85p1IsBbfF\"}";
        String json = TemplateTestJsonUtil.parseToJson(resourceCreated);
        assertEquals(expectedJson, json);

        ResourceCreated resourceCreatedTwo = ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals(workflowStepName, resourceCreatedTwo.workflowStepName());
        assertEquals("workflow_step_1", resourceCreatedTwo.workflowStepId());
        assertEquals("L85p1IsBbfF", resourceCreatedTwo.resourceId());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"wrong\":\"A\",\"resource_id\":\"B\"}";
        IOException badJsonException = assertThrows(
            IOException.class,
            () -> ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(badJson))
        );
        assertEquals("Unable to parse field [wrong] in a resources_created object.", badJsonException.getMessage());

        String missingJson = "{\"resource_id\":\"B\"}";
        FlowFrameworkException missingJsonException = assertThrows(
            FlowFrameworkException.class,
            () -> ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(missingJson))
        );
        assertEquals("A ResourceCreated object requires workflowStepName", missingJsonException.getMessage());
    }

}
