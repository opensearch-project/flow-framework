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
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.junit.Before;
import org.junit.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

public class TenantAwareHelperTests {

    @Mock
    private ActionListener<?> actionListener;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testValidateTenantId_MultiTenancyEnabled_TenantIdNull() {
        boolean result = TenantAwareHelper.validateTenantId(true, null, actionListener);
        assertFalse(result);
        ArgumentCaptor<FlowFrameworkException> captor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(actionListener).onFailure(captor.capture());
        FlowFrameworkException exception = captor.getValue();
        assert exception.status() == RestStatus.FORBIDDEN;
        assert exception.getMessage().equals("You don't have permission to access this resource");
    }

    @Test
    public void testValidateTenantId_MultiTenancyEnabled_TenantIdPresent() {
        assertTrue(TenantAwareHelper.validateTenantId(true, "_tenant_id", actionListener));
    }

    @Test
    public void testValidateTenantId_MultiTenancyDisabled() {
        assertTrue(TenantAwareHelper.validateTenantId(false, null, actionListener));
    }

    @Test
    public void testValidateTenantResource_MultiTenancyEnabled_TenantIdMismatch() {
        boolean result = TenantAwareHelper.validateTenantResource(true, null, "different_tenant_id", actionListener);
        assertFalse(result);
        ArgumentCaptor<FlowFrameworkException> captor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(actionListener).onFailure(captor.capture());
        FlowFrameworkException exception = captor.getValue();
        assert exception.status() == RestStatus.FORBIDDEN;
        assert exception.getMessage().equals("You don't have permission to access this resource");
    }

    @Test
    public void testValidateTenantResource_MultiTenancyEnabled_TenantIdMatch() {
        assertTrue(TenantAwareHelper.validateTenantResource(true, "_tenant_id", "_tenant_id", actionListener));
    }

    @Test
    public void testValidateTenantResource_MultiTenancyDisabled() {
        assertTrue(TenantAwareHelper.validateTenantResource(false, "_tenant_id", "different_tenant_id", actionListener));
    }
}
