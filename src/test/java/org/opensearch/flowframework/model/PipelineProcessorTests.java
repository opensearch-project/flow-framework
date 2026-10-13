/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class PipelineProcessorTests extends OpenSearchTestCase {

    public void testProcessor() throws IOException {
        PipelineProcessor processor = new PipelineProcessor("foo", Map.of("bar", "baz"));

        assertEquals("foo", processor.type());
        assertEquals(Map.of("bar", "baz"), processor.params());

        String expectedJson = "{\"type\":\"foo\",\"params\":{\"bar\":\"baz\"}}";
        String json = TemplateTestJsonUtil.parseToJson(processor);
        assertEquals(expectedJson, json);

        PipelineProcessor processorX = PipelineProcessor.parse(TemplateTestJsonUtil.jsonToParser(json));
        assertEquals("foo", processorX.type());
        assertEquals(Map.of("bar", "baz"), processorX.params());
    }

    public void testExceptions() throws IOException {
        String badJson = "{\"badField\":\"foo\",\"params\":{\"bar\":\"baz\"}}";
        FlowFrameworkException e = assertThrows(
            FlowFrameworkException.class,
            () -> PipelineProcessor.parse(TemplateTestJsonUtil.jsonToParser(badJson))
        );
        assertEquals("Unable to parse field [badField] in a pipeline processor object.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());

        String noTypeJson = "{\"params\":{\"bar\":\"baz\"}}";
        e = assertThrows(FlowFrameworkException.class, () -> PipelineProcessor.parse(TemplateTestJsonUtil.jsonToParser(noTypeJson)));
        assertEquals("A processor object requires a type field.", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, e.getRestStatus());
    }

}
