/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;

public class TenantAwareHelperTests extends OpenSearchTestCase {

    private static final String NO_PERMISSION_TO_ACCESS = "No permission to access this resource";
    @Mock
    private ActionListener<?> actionListener;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testValidateTenantId_MultiTenancyEnabled_TenantIdNull() {
        boolean result = TenantAwareHelper.validateTenantId(true, null, actionListener);
        assertFalse(result);
        ArgumentCaptor<FlowFrameworkException> captor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(actionListener).onFailure(captor.capture());
        FlowFrameworkException exception = captor.getValue();
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals(NO_PERMISSION_TO_ACCESS, exception.getMessage());
    }

    public void testValidateTenantId_MultiTenancyEnabled_TenantIdPresent() {
        assertTrue(TenantAwareHelper.validateTenantId(true, "_tenant_id", actionListener));
    }

    public void testValidateTenantId_MultiTenancyDisabled() {
        assertTrue(TenantAwareHelper.validateTenantId(false, null, actionListener));
    }

    public void testValidateTenantResource_MultiTenancyEnabled_TenantIdMismatch() {
        boolean result = TenantAwareHelper.validateTenantResource(true, null, "different_tenant_id", actionListener);
        assertFalse(result);
        ArgumentCaptor<FlowFrameworkException> captor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(actionListener).onFailure(captor.capture());
        FlowFrameworkException exception = captor.getValue();
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals(NO_PERMISSION_TO_ACCESS, exception.getMessage());
    }

    public void testValidateTenantResource_MultiTenancyEnabled_TenantIdMatch() {
        assertTrue(TenantAwareHelper.validateTenantResource(true, "_tenant_id", "_tenant_id", actionListener));
    }

    public void testValidateTenantResource_MultiTenancyDisabled() {
        assertTrue(TenantAwareHelper.validateTenantResource(false, "_tenant_id", "different_tenant_id", actionListener));
    }

    public void testGetTenantID() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(tenantId));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest);
        assertEquals(tenantId, actualTenantID);
    }

    public void testGetTenantID_NullTenantID() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(null));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        try {
            TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest);
            Assert.fail("Expected FlowFrameworkException");
        } catch (Exception e) {
            assertTrue(e instanceof FlowFrameworkException);
            assertEquals(RestStatus.FORBIDDEN, ((FlowFrameworkException) e).status());
            assertEquals("Tenant ID can't be null", e.getMessage());
        }
    }

    public void testGetTenantID_NoMultiTenancy() {
        String tenantId = "test-tenant";
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(tenantId));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String tenantID = TenantAwareHelper.getTenantID(Boolean.FALSE, restRequest);
        assertNull(tenantID);
    }

    public void testGetTenantID_EmptyTenantIDList() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.emptyList());
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest)
        );
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testGetTenantID_MissingTenantIDHeader() {
        Map<String, List<String>> headers = new HashMap<>();
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest)
        );
        assertEquals(RestStatus.FORBIDDEN, exception.status());
        assertEquals("Tenant ID header is missing or has no value", exception.getMessage());
    }

    public void testGetTenantID_MultipleValues() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, List.of("tenant1", "tenant2"));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest);
        assertEquals("tenant1", actualTenantID);
    }

    public void testGetTenantID_EmptyStringTenantID() {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(CommonValue.TENANT_ID_HEADER, Collections.singletonList(""));
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();

        String actualTenantID = TenantAwareHelper.getTenantID(Boolean.TRUE, restRequest);
        assertEquals("", actualTenantID);
    }

}
