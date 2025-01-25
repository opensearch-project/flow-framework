/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReprovisionWorkflowRequestTests extends OpenSearchTestCase {

    private Template originalTemplate;
    private Template updatedTemplate;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Collections.emptyMap(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowNode nodeC = new WorkflowNode("C", "c-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        WorkflowEdge edgeBC = new WorkflowEdge("B", "C");
        Workflow originalWorkflow = new Workflow(Map.of("key", "value"), List.of(nodeA, nodeB), List.of(edgeAB));
        Workflow updatedWorkflow = new Workflow(Map.of("key", "value"), List.of(nodeA, nodeB, nodeC), List.of(edgeAB, edgeBC));

        this.originalTemplate = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", originalWorkflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null
        );

        this.updatedTemplate = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", updatedWorkflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null
        );
    }

    public void testReprovisionWorkflowRequest() throws IOException {
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest("123", originalTemplate, updatedTemplate, TimeValue.MINUS_ONE);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));

        ReprovisionWorkflowRequest requestFromStreamInput = new ReprovisionWorkflowRequest(in);
        assertEquals(request.getWorkflowId(), requestFromStreamInput.getWorkflowId());
        assertEquals(request.getOriginalTemplate().toJson(), requestFromStreamInput.getOriginalTemplate().toJson());
        assertEquals(request.getUpdatedTemplate().toJson(), requestFromStreamInput.getUpdatedTemplate().toJson());
    }

}
