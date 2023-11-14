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

public class ResourcesCreatedTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testParseFeature() throws IOException {
        ResourcesCreated resourcesCreated = new ResourcesCreated("A", "B");
        assertEquals(resourcesCreated.workflowStepName(), "A");
        assertEquals(resourcesCreated.resourceId(), "B");

        String expectedJson = "{\"workflow_step_name\":\"A\",\"resource_id\":\"B\"}";
        String json = TemplateTestJsonUtil.parseToJson(resourcesCreated);
        assertEquals(expectedJson, json);

        ResourcesCreated resourcesCreatedTwo = ResourcesCreated.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals("A", resourcesCreatedTwo.workflowStepName());
        assertEquals("B", resourcesCreatedTwo.resourceId());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"wrong\":\"A\",\"resource_id\":\"B\"}";
        IOException e = assertThrows(IOException.class, () -> ResourcesCreated.parse(TemplateTestJsonUtil.jsonToParser(badJson)));
        assertEquals("Unable to parse field [wrong] in a resources_created object.", e.getMessage());

        String missingJson = "{\"resource_id\":\"B\"}";
        e = assertThrows(IOException.class, () -> ResourcesCreated.parse(TemplateTestJsonUtil.jsonToParser(missingJson)));
        assertEquals("A resourcesCreated object requires both a workflowStepName and resourceId.", e.getMessage());
    }

}
