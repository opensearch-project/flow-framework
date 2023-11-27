/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

public class TestHelpers {

    public static User randomUser() {
        return new User(
            randomAlphaOfLength(8),
            ImmutableList.of(randomAlphaOfLength(10)),
            ImmutableList.of("all_access"),
            ImmutableList.of("attribute=test")
        );
    }

    public static ClusterSettings clusterSetting(Settings settings, Setting<?>... setting) {
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Sets.newHashSet(setting).stream()
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        return clusterSettings;
    }

    public static XContentBuilder builder() throws IOException {
        return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    public static Map<String, Object> XContentBuilderToMap(XContentBuilder builder) {
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
