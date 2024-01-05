/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowFrameworkSettingsTests extends OpenSearchTestCase {
    private Settings settings;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;

    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.builder().build();
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED,
                FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY,
                FlowFrameworkSettings.MAX_WORKFLOW_STEPS
            )
        ).collect(Collectors.toSet());
        clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        flowFrameworkSettings = new FlowFrameworkSettings(clusterService, settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSettings() throws IOException {
        assertFalse(flowFrameworkSettings.isFlowFrameworkEnabled());
        assertEquals(Optional.of(5), Optional.ofNullable(flowFrameworkSettings.getMaxRetry()));
        assertEquals(Optional.of(50), Optional.ofNullable(flowFrameworkSettings.getMaxWorkflowSteps()));
    }
}
