/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ResourceCreatedTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testParseFeature() throws IOException {
        String workflowStepName = WorkflowResources.CREATE_CONNECTOR.getWorkflowStep();
        ResourceCreated ResourceCreated = new ResourceCreated(workflowStepName, "workflow_step_1", "L85p1IsBbfF");
        assertEquals(ResourceCreated.workflowStepName(), workflowStepName);
        assertEquals(ResourceCreated.workflowStepId(), "workflow_step_1");
        assertEquals(ResourceCreated.resourceId(), "L85p1IsBbfF");

        String expectedJson =
            "{\"workflow_step_name\":\"create_connector\",\"workflow_step_id\":\"workflow_step_1\",\"connector_id\":\"L85p1IsBbfF\"}";
        String json = TemplateTestJsonUtil.parseToJson(ResourceCreated);
        assertEquals(expectedJson, json);

        ResourceCreated ResourceCreatedTwo = ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals(workflowStepName, ResourceCreatedTwo.workflowStepName());
        assertEquals("workflow_step_1", ResourceCreatedTwo.workflowStepId());
        assertEquals("L85p1IsBbfF", ResourceCreatedTwo.resourceId());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"wrong\":\"A\",\"resource_id\":\"B\"}";
        IOException e = assertThrows(IOException.class, () -> ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(badJson)));
        assertEquals("Unable to parse field [wrong] in a resources_created object.", e.getMessage());

        String missingJson = "{\"resource_id\":\"B\"}";
        e = assertThrows(IOException.class, () -> ResourceCreated.parse(TemplateTestJsonUtil.jsonToParser(missingJson)));
        assertEquals("Unable to parse field [resource_id] in a resources_created object.", e.getMessage());
    }

}
