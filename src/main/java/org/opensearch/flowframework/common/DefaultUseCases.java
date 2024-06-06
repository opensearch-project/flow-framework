/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_ACCESS_KEY;
import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_KEY;
import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_SECRET_KEY;
import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_SESSION_TOKEN;
import static org.opensearch.flowframework.common.CommonValue.CREATE_INGEST_PIPELINE_MODEL_ID;

/**
 * Enum encapsulating the different default use cases and templates we have stored
 */
public enum DefaultUseCases {

    /** defaults file and substitution ready template for OpenAI embedding model */
    OPEN_AI_EMBEDDING_MODEL_DEPLOY(
        "open_ai_embedding_model_deploy",
        "defaults/openai-embedding-defaults.json",
        "substitutionTemplates/deploy-remote-model-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for Cohere embedding model */
    COHERE_EMBEDDING_MODEL_DEPLOY(
        "cohere_embedding_model_deploy",
        "defaults/cohere-embedding-defaults.json",
        "substitutionTemplates/deploy-remote-model-extra-params-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for Bedrock Titan embedding model */
    BEDROCK_TITAN_EMBEDDING_MODEL_DEPLOY(
        "bedrock_titan_embedding_model_deploy",
        "defaults/bedrock-titan-embedding-defaults.json",
        "substitutionTemplates/deploy-remote-bedrock-model-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_ACCESS_KEY, CREATE_CONNECTOR_CREDENTIAL_SECRET_KEY, CREATE_CONNECTOR_CREDENTIAL_SESSION_TOKEN)
    ),
    /** defaults file and substitution ready template for Bedrock Titan multimodal embedding model */
    BEDROCK_TITAN_MULTIMODAL_MODEL_DEPLOY(
        "bedrock_titan_multimodal_model_deploy",
        "defaults/bedrock-titan-multimodal-defaults.json",
        "substitutionTemplates/deploy-remote-bedrock-model-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_ACCESS_KEY, CREATE_CONNECTOR_CREDENTIAL_SECRET_KEY, CREATE_CONNECTOR_CREDENTIAL_SESSION_TOKEN)
    ),
    /** defaults file and substitution ready template for Cohere chat model */
    COHERE_CHAT_MODEL_DEPLOY(
        "cohere_chat_model_deploy",
        "defaults/cohere-chat-defaults.json",
        "substitutionTemplates/deploy-remote-model-chat-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for OpenAI chat model */
    OPENAI_CHAT_MODEL_DEPLOY(
        "openai_chat_model_deploy",
        "defaults/openai-chat-defaults.json",
        "substitutionTemplates/deploy-remote-model-chat-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for local neural sparse model and ingest pipeline*/
    LOCAL_NEURAL_SPARSE_SEARCH_BI_ENCODER(
        "local_neural_sparse_search_bi_encoder",
        "defaults/local-sparse-search-biencoder-defaults.json",
        "substitutionTemplates/neural-sparse-local-biencoder-template.json",
        Collections.emptyList()
    ),
    /** defaults file and substitution ready template for semantic search, no model creation*/
    SEMANTIC_SEARCH(
        "semantic_search",
        "defaults/semantic-search-defaults.json",
        "substitutionTemplates/semantic-search-template.json",
        List.of(CREATE_INGEST_PIPELINE_MODEL_ID)
    ),
    /** defaults file and substitution ready template for multimodal search, no model creation*/
    MULTI_MODAL_SEARCH(
        "multimodal_search",
        "defaults/multi-modal-search-defaults.json",
        "substitutionTemplates/multi-modal-search-template.json",
        List.of(CREATE_INGEST_PIPELINE_MODEL_ID)
    ),
    /** defaults file and substitution ready template for multimodal search, no model creation*/
    MULTI_MODAL_SEARCH_WITH_BEDROCK_TITAN(
        "multimodal_search_with_bedrock_titan",
        "defaults/multimodal-search-bedrock-titan-defaults.json",
        "substitutionTemplates/multi-modal-search-with-bedrock-titan-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_ACCESS_KEY, CREATE_CONNECTOR_CREDENTIAL_SECRET_KEY, CREATE_CONNECTOR_CREDENTIAL_SESSION_TOKEN)
    ),
    /** defaults file and substitution ready template for semantic search with query enricher processor attached, no model creation*/
    SEMANTIC_SEARCH_WITH_QUERY_ENRICHER(
        "semantic_search_with_query_enricher",
        "defaults/semantic-search-query-enricher-defaults.json",
        "substitutionTemplates/semantic-search-with-query-enricher-template.json",
        List.of(CREATE_INGEST_PIPELINE_MODEL_ID)
    ),
    /** defaults file and substitution ready template for semantic search with cohere embedding model*/
    SEMANTIC_SEARCH_WITH_COHERE_EMBEDDING(
        "semantic_search_with_cohere_embedding",
        "defaults/cohere-embedding-semantic-search-defaults.json",
        "substitutionTemplates/semantic-search-with-model-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for semantic search with query enricher processor attached and cohere embedding model*/
    SEMANTIC_SEARCH_WITH_COHERE_EMBEDDING_AND_QUERY_ENRICHER(
        "semantic_search_with_cohere_embedding_query_enricher",
        "defaults/cohere-embedding-semantic-search-with-query-enricher-defaults.json",
        "substitutionTemplates/semantic-search-with-model-and-query-enricher-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for hybrid search, no model creation*/
    HYBRID_SEARCH(
        "hybrid_search",
        "defaults/hybrid-search-defaults.json",
        "substitutionTemplates/hybrid-search-template.json",
        List.of(CREATE_INGEST_PIPELINE_MODEL_ID)
    ),
    /** defaults file and substitution ready template for conversational search with cohere chat model*/
    CONVERSATIONAL_SEARCH_WITH_COHERE_DEPLOY(
        "conversational_search_with_llm_deploy",
        "defaults/conversational-search-defaults.json",
        "substitutionTemplates/conversational-search-with-cohere-model-template.json",
        List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
    ),
    /** defaults file and substitution ready template for semantic search with a local pretrained model*/
    SEMANTIC_SEARCH_WITH_LOCAL_MODEL(
        "semantic_search_with_local_model",
        "defaults/semantic-search-with-local-model-defaults.json",
        "substitutionTemplates/semantic-search-with-local-model-template.json",
        Collections.emptyList()

    ),
    /** defaults file and substitution ready template for hybrid search with a local pretrained model*/
    HYBRID_SEARCH_WITH_LOCAL_MODEL(
        "hybrid_search_with_local_model",
        "defaults/hybrid-search-with-local-model-defaults.json",
        "substitutionTemplates/hybrid-search-with-local-model-template.json",
        Collections.emptyList()
    );

    private final String useCaseName;
    private final String defaultsFile;
    private final String substitutionReadyFile;
    private final List<String> requiredParams;
    private static final Logger logger = LogManager.getLogger(DefaultUseCases.class);

    DefaultUseCases(String useCaseName, String defaultsFile, String substitutionReadyFile, List<String> requiredParams) {
        this.useCaseName = useCaseName;
        this.defaultsFile = defaultsFile;
        this.substitutionReadyFile = substitutionReadyFile;
        this.requiredParams = requiredParams;
    }

    /**
     * Returns the useCaseName for the given enum Constant
     * @return the useCaseName of this use case.
     */
    public String getUseCaseName() {
        return useCaseName;
    }

    /**
     * Returns the defaultsFile for the given enum Constant
     * @return the defaultsFile of this for the given useCase.
     */
    public String getDefaultsFile() {
        return defaultsFile;
    }

    /**
     * Returns the substitutionReadyFile for the given enum Constant
     * @return the substitutionReadyFile of the given useCase
     */
    public String getSubstitutionReadyFile() {
        return substitutionReadyFile;
    }

    /**
     * Returns the required params for the given enum Constant
     * @return the required params of the given useCase
     */
    public List<String> getRequiredParams() {
        return requiredParams;
    }

    /**
     * Gets the defaultsFile based on the given use case.
     * @param useCaseName name of the given use case
     * @return the defaultsFile for that usecase
     * @throws FlowFrameworkException if the use case doesn't exist in enum
     */
    public static String getDefaultsFileByUseCaseName(String useCaseName) throws FlowFrameworkException {
        if (useCaseName != null && !useCaseName.isEmpty()) {
            for (DefaultUseCases usecase : values()) {
                if (useCaseName.equals(usecase.getUseCaseName())) {
                    return usecase.getDefaultsFile();
                }
            }
        }
        logger.error("Unable to find defaults file for use case: {}", useCaseName);
        throw new FlowFrameworkException("Unable to find defaults file for use case: " + useCaseName, RestStatus.BAD_REQUEST);
    }

    /**
     * Gets the substitutionReadyFile based on the given use case
     * @param useCaseName name of the given use case
     * @return the substitutionReadyFile which has the template
     * @throws FlowFrameworkException if the use case doesn't exist in enum
     */
    public static String getSubstitutionReadyFileByUseCaseName(String useCaseName) throws FlowFrameworkException {
        if (useCaseName != null && !useCaseName.isEmpty()) {
            for (DefaultUseCases useCase : values()) {
                if (useCase.getUseCaseName().equals(useCaseName)) {
                    return useCase.getSubstitutionReadyFile();
                }
            }
        }
        logger.error("Unable to find substitution ready file for use case: {}", useCaseName);
        throw new FlowFrameworkException("Unable to find substitution ready file for use case: " + useCaseName, RestStatus.BAD_REQUEST);
    }

    /**
     * Gets the required parameters based on the given use case
     * @param useCaseName name of the given use case
     * @return the list of required params
     */
    public static List<String> getRequiredParamsByUseCaseName(String useCaseName) {
        if (useCaseName != null && !useCaseName.isEmpty()) {
            for (DefaultUseCases useCase : values()) {
                if (useCase.getUseCaseName().equals(useCaseName)) {
                    return new ArrayList<String>(useCase.getRequiredParams());
                }
            }
        }
        logger.error("Default use case [" + useCaseName + "] does not exist");
        throw new FlowFrameworkException("Default use case [" + useCaseName + "] does not exist", RestStatus.BAD_REQUEST);
    }
}
