/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.AdminClient;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;

public class CreateIndexStep implements Workflow {

    AdminClient adminClient;

    @Override
    public CompletableFuture<Workflow> execute() throws Exception {

        // ActionListener<CreateIndexResponse> actionListener
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(getIndexMappings(fileName), XContentType.JSON)
            .settings(settings);
        adminClient.indices().create(request, actionListener);

    }

    /**
     * Get index mapping json content.
     *
     * @return index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getIndexMappings(String mappingFileName) throws IOException {
        URL url = CreateIndexStep.class.getClassLoader().getResource(mappingFileName);
        return Resources.toString(url, Charsets.UTF_8);
    }
}
