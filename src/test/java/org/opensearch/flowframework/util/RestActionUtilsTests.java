/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Assert;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestActionUtilsTests extends OpenSearchTestCase {

    public void testGetTenantID() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(tenantId));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = RestActionUtils.getTenantID(Boolean.TRUE, restRequest);
        Assert.assertEquals(tenantId, actualTenantID);
    }

    public void testGetTenantID_NullTenantID() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(null));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        try {
            RestActionUtils.getTenantID(Boolean.TRUE, restRequest);
            Assert.fail("Expected FlowFrameworkException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof FlowFrameworkException);
            Assert.assertEquals(RestStatus.FORBIDDEN, ((FlowFrameworkException) e).status());
            Assert.assertEquals("Tenant ID can't be null", e.getMessage());
        }
    }

    public void testGetTenantID_NoMultiTenancy() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(tenantId));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String tenantID = RestActionUtils.getTenantID(Boolean.FALSE, restRequest);
        Assert.assertNull(tenantID);
    }

    public void testGetTenantID_EmptyTenantIDList() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.emptyList());
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> RestActionUtils.getTenantID(Boolean.TRUE, restRequest)
        );
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is present but has no value", exception.getMessage());
    }

    public void testGetTenantID_MissingTenantIDHeader() {
        Map<String, List<String>> headers = new HashMap<>();
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> RestActionUtils.getTenantID(Boolean.TRUE, restRequest)
        );
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is missing", exception.getMessage());
    }

    public void testGetTenantID_MultipleValues() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, List.of("tenant1", "tenant2"));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = RestActionUtils.getTenantID(Boolean.TRUE, restRequest);
        assertEquals("tenant1", actualTenantID);
    }

    public void testGetTenantID_EmptyStringTenantID() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(""));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = RestActionUtils.getTenantID(Boolean.TRUE, restRequest);
        assertEquals("", actualTenantID);
    }
}
