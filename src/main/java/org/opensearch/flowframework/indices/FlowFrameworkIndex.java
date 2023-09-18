/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import static org.opensearch.flowframework.constant.CommonName.GLOBAL_CONTEXT_INDEX_NAME;
import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX_SCHEMA_VERSION;

public enum FlowFrameworkIndex {
    GLOBAL_CONTEXT(
            GLOBAL_CONTEXT_INDEX_NAME,
            GLOBAL_CONTEXT_INDEX_MAPPING,
            GLOBAL_CONTEXT_INDEX_SCHEMA_VERSION
    );

    private final String indexName;
    private final String mapping;
    private final Integer version;

    FlowFrameworkIndex(String name, String mapping, Integer version) {
        this.indexName = name;
        this.mapping = mapping;
        this.version = version;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMapping() {
        return mapping;
    }

    public Integer getVersion() {
        return version;
    }
}
