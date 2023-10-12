/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.client;

import org.opensearch.client.Client;
import org.opensearch.ml.client.MachineLearningNodeClient;

/**
 Class to initiate an instance of MLClient
 */
public class MLClient {
    private static MachineLearningNodeClient INSTANCE;

    private MLClient() {}

    /**
     * Creates machine learning client.
     *
     * @param client client of OpenSearch.
     * @return machine learning client from ml-commons.
     */
    public static MachineLearningNodeClient createMLClient(Client client) {
        if (INSTANCE == null) {
            INSTANCE = new MachineLearningNodeClient(client);
        }
        return INSTANCE;
    }
}
