/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.opensearch.flowframework.common.ThrowingSupplierWrapper;

import java.util.function.Supplier;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX_VERSION;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX_VERSION;

/**
 * An enumeration of Flow Framework indices
 */
public enum FlowFrameworkIndex {
    /**
     * Global Context Index
    */
    GLOBAL_CONTEXT(
        GLOBAL_CONTEXT_INDEX,
        ThrowingSupplierWrapper.throwingSupplierWrapper(FlowFrameworkIndicesHandler::getGlobalContextMappings),
        GLOBAL_CONTEXT_INDEX_VERSION
    ),
    WORKFLOW_STATE(
        WORKFLOW_STATE_INDEX,
        ThrowingSupplierWrapper.throwingSupplierWrapper(FlowFrameworkIndicesHandler::getGlobalContextMappings),
        WORKFLOW_STATE_INDEX_VERSION
    );

    private final String indexName;
    private final String mapping;
    private final Integer version;

    FlowFrameworkIndex(String name, Supplier<String> mappingSupplier, Integer version) {
        this.indexName = name;
        this.mapping = mappingSupplier.get();
        this.version = version;
    }

    /**
     * Retrieves the index name
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Retrieves the index mapping
     * @return the index mapping
     */
    public String getMapping() {
        return mapping;
    }

    /**
     * Retrieves the index version
     * @return the index version
     */
    public Integer getVersion() {
        return version;
    }
}
