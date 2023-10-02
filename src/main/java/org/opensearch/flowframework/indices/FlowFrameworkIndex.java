/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.opensearch.flowframework.function.ThrowingSupplierWrapper;

import java.util.function.Supplier;

import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX_VERSION;

/**
 * An enumeration of Flow Framework indices
 */
public enum FlowFrameworkIndex {
    GLOBAL_CONTEXT(
        GLOBAL_CONTEXT_INDEX,
        ThrowingSupplierWrapper.throwingSupplierWrapper(GlobalContextHandler::getGlobalContextMappings),
        GLOBAL_CONTEXT_INDEX_VERSION
    );

    private final String indexName;
    private final String mapping;
    private final Integer version;

    FlowFrameworkIndex(String name, Supplier<String> mappingSupplier, Integer version) {
        this.indexName = name;
        this.mapping = mappingSupplier.get();
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
