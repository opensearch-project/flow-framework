/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.http.HttpHost;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.flowframework.common.CommonValue.HOSTNAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.HTTP_HOST_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PORT_FIELD;
import static org.opensearch.flowframework.common.CommonValue.SCHEME_FIELD;

public class HttpHostStepTests extends OpenSearchTestCase {

    public void testHttpHost() throws InterruptedException, ExecutionException {
        HttpHostStep httpHostStep = new HttpHostStep();
        assertEquals(HttpHostStep.NAME, httpHostStep.getName());

        WorkflowData inputData = new WorkflowData(
            Map.ofEntries(Map.entry(SCHEME_FIELD, "http"), Map.entry(HOSTNAME_FIELD, "localhost"), Map.entry(PORT_FIELD, 1234)),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = httpHostStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        assertEquals(HttpHost.class, future.get().getContent().get(HTTP_HOST_FIELD).getClass());
        HttpHost host = (HttpHost) future.get().getContent().get(HTTP_HOST_FIELD);
        assertEquals("http", host.getSchemeName());
        assertEquals("localhost", host.getHostName());
        assertEquals(1234, host.getPort());
    }

    public void testBadScheme() {
        HttpHostStep httpHostStep = new HttpHostStep();

        WorkflowData badSchemeData = new WorkflowData(
            Map.ofEntries(Map.entry(SCHEME_FIELD, "ftp"), Map.entry(HOSTNAME_FIELD, "localhost"), Map.entry(PORT_FIELD, 1234)),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = httpHostStep.execute(
            badSchemeData.getNodeId(),
            badSchemeData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(FlowFrameworkException.class, ex.getCause().getClass());
        assertEquals("http_host scheme must be http or https", ex.getCause().getMessage());
    }

    public void testBadPort() {
        HttpHostStep httpHostStep = new HttpHostStep();

        WorkflowData badPortData = new WorkflowData(
            Map.ofEntries(Map.entry(SCHEME_FIELD, "https"), Map.entry(HOSTNAME_FIELD, "localhost"), Map.entry(PORT_FIELD, 123456)),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = httpHostStep.execute(
            badPortData.getNodeId(),
            badPortData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(FlowFrameworkException.class, ex.getCause().getClass());
        assertEquals("http_host port number must be between 0 and 65535", ex.getCause().getMessage());
    }

    public void testNoParsePort() {
        HttpHostStep httpHostStep = new HttpHostStep();

        WorkflowData noParsePortData = new WorkflowData(
            Map.ofEntries(Map.entry(SCHEME_FIELD, "https"), Map.entry(HOSTNAME_FIELD, "localhost"), Map.entry(PORT_FIELD, "doesn't parse")),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = httpHostStep.execute(
            noParsePortData.getNodeId(),
            noParsePortData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(FlowFrameworkException.class, ex.getCause().getClass());
        assertEquals("http_host port must be a number between 0 and 65535", ex.getCause().getMessage());
    }
}
