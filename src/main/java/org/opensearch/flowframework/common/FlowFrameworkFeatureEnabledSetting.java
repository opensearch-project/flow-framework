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
import org.opensearch.common.settings.Settings;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;

/**
 * Controls enabling or disabling features of this plugin
 */
public class FlowFrameworkFeatureEnabledSetting {

    private volatile Boolean isFlowFrameworkEnabled;

    /**
     * Instantiate this class.
     *
     * @param clusterService OpenSearch cluster service
     * @param settings OpenSearch settings
     */
    public FlowFrameworkFeatureEnabledSetting(ClusterService clusterService, Settings settings) {
        // Currently this is just an on/off switch for the entire plugin's API.
        // If desired more fine-tuned feature settings can be added below.
        isFlowFrameworkEnabled = FLOW_FRAMEWORK_ENABLED.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FLOW_FRAMEWORK_ENABLED, it -> isFlowFrameworkEnabled = it);
    }

    /**
    * Whether the flow framework feature is enabled. If disabled, no REST APIs will be availble.
    * @return whether Flow Framework is enabled.
    */
    public boolean isFlowFrameworkEnabled() {
        return isFlowFrameworkEnabled;
    }
}
