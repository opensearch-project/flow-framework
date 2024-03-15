/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

public class DefaultUseCasesTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testGetDefaultsFileByValidUseCaseName() throws FlowFrameworkException {
        String defaultsFile = DefaultUseCases.getDefaultsFileByUseCaseName("open_ai_embedding_model_deploy");
        assertEquals("defaults/open-ai-embedding-defaults.json", defaultsFile);
    }

    public void testGetDefaultsFileByInvalidUseCaseName() throws FlowFrameworkException {
        FlowFrameworkException e = assertThrows(
            FlowFrameworkException.class,
            () -> DefaultUseCases.getDefaultsFileByUseCaseName("invalid_use_case")
        );
    }

    public void testGetSubstitutionTemplateByValidUseCaseName() throws FlowFrameworkException {
        String templateFile = DefaultUseCases.getSubstitutionReadyFileByUseCaseName("open_ai_embedding_model_deploy");
        assertEquals("substitutionTemplates/deploy-remote-model-template.json", templateFile);
    }

    public void testGetSubstitutionTemplateByInvalidUseCaseName() throws FlowFrameworkException {
        FlowFrameworkException e = assertThrows(
            FlowFrameworkException.class,
            () -> DefaultUseCases.getSubstitutionReadyFileByUseCaseName("invalid_use_case")
        );
    }
}
