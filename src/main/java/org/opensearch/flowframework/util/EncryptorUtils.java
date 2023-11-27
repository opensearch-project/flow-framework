/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;

import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;

import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY;
import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY_INDEX;

/**
 * Encryption utility class
 */
public class EncryptorUtils {

    private static final Logger logger = LogManager.getLogger(EncryptorUtils.class);

    private ClusterService clusterService;
    private Client client;
    private String masterKey;

    /**
     * Instantiates a new EncryptorUtils object
     * @param clusterService the cluster service
     * @param client the node client
     */
    public EncryptorUtils(ClusterService clusterService, Client client) {
        this.masterKey = null;
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Sets the master key
     * @param masterKey the master key
     */
    protected void setMasterKey(String masterKey) {
        this.masterKey = masterKey;
    }

    /**
     * Returns the master key
     * @return the master key
     */
    protected String getMasterKey() {
        return this.masterKey;
    }

    /**
     * Randomly generate a master key
     * @return the master key string
     */
    protected String generateMasterKey() {
        byte[] masterKeyBytes = new byte[32];
        new SecureRandom().nextBytes(masterKeyBytes);
        return Base64.getEncoder().encodeToString(masterKeyBytes);
    }

    /**
     * Encrypts template credentials
     * @param template the template to encrypt
     * @return template with encrypted credentials
     */
    public Template encryptTemplateCredentials(Template template) {
        return processTemplateCredentials(template, this::encrypt);
    }

    /**
     * Decrypts template credentials
     * @param template the template to decrypt
     * @return template with decrypted credentials
     */
    public Template decryptTemplateCredentials(Template template) {
        return processTemplateCredentials(template, this::decrypt);
    }

    /**
     * Applies the given cipher function on template credentials
     * @param template the template to process
     * @param cipher the encryption/decryption function to apply on credential values
     * @return template with encrypted credentials
     */
    private Template processTemplateCredentials(Template template, Function<String, String> cipherFunction) {
        Template.Builder processedTemplateBuilder = new Template.Builder();

        Map<String, Workflow> processedWorkflows = new HashMap<>();
        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (node.userInputs().containsKey(CREDENTIAL_FIELD)) {
                    // Apply the cipher funcion on all values within credential field
                    @SuppressWarnings("unchecked")
                    Map<String, String> credentials = (Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD);
                    credentials.replaceAll((key, cred) -> cipherFunction.apply(cred));

                    // Replace credentials field in node user inputs
                    Map<String, Object> encryptedUserInputs = new HashMap<>();
                    encryptedUserInputs.putAll(node.userInputs());
                    encryptedUserInputs.replace(CREDENTIAL_FIELD, credentials);

                    // build new node to add to processed nodes
                    WorkflowNode encryptedWorkflowNode = new WorkflowNode(
                        node.id(),
                        node.type(),
                        node.previousNodeInputs(),
                        encryptedUserInputs
                    );
                    processedNodes.add(encryptedWorkflowNode);
                } else {
                    processedNodes.add(node);
                }
            }

            // Add processed workflow nodes to processed workflows
            processedWorkflows.put(entry.getKey(), new Workflow(entry.getValue().userParams(), processedNodes, entry.getValue().edges()));
        }

        Template processedTemplate = processedTemplateBuilder.name(template.name())
            .description(template.description())
            .useCase(template.useCase())
            .templateVersion(template.templateVersion())
            .compatibilityVersion(template.compatibilityVersion())
            .workflows(processedWorkflows)
            .uiMetadata(template.getUiMetadata())
            .user(template.getUser())
            .build();

        return processedTemplate;
    }

    /**
     * Encrypts the given credential
     * @param credential the credential to encrypt
     * @return the encrypted credential
     */
    protected String encrypt(final String credential) {
        initializeMasterKeyIfAbsent();
        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        byte[] bytes = Base64.getDecoder().decode(masterKey);
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(new SecretKeySpec(bytes, "AES"), "Custom", "", "AES/GCM/NoPadding");
        final CryptoResult<byte[], JceMasterKey> encryptResult = crypto.encryptData(
            jceMasterKey,
            credential.getBytes(StandardCharsets.UTF_8)
        );
        return Base64.getEncoder().encodeToString(encryptResult.getResult());
    }

    /**
     * Decrypts the given credential
     * @param encryptedCredential the credential to decrypt
     * @return the decrypted credential
     */
    protected String decrypt(final String encryptedCredential) {
        initializeMasterKeyIfAbsent();
        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();

        byte[] bytes = Base64.getDecoder().decode(masterKey);
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(new SecretKeySpec(bytes, "AES"), "Custom", "", "AES/GCM/NoPadding");

        final CryptoResult<byte[], JceMasterKey> decryptedResult = crypto.decryptData(
            jceMasterKey,
            Base64.getDecoder().decode(encryptedCredential)
        );
        return new String(decryptedResult.getResult(), StandardCharsets.UTF_8);
    }

    /**
     * Retrieves an existing master key or generates a new key to index
     * @param listener the action listener
     */
    public void initializeMasterKey(ActionListener<Boolean> listener) {
        // Index has either been created or it already exists, need to check if master key has been initalized already, if not then
        // generate
        // This is necessary in case of global context index restoration from snapshot, will need to use the same master key to decrypt
        // stored credentials
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

            GetRequest getRequest = new GetRequest(MASTER_KEY_INDEX).id(MASTER_KEY);
            client.get(getRequest, ActionListener.wrap(getResponse -> {

                if (!getResponse.isExists()) {

                    // Generate new key and index
                    final String masterKey = generateMasterKey();
                    IndexRequest masterKeyIndexRequest = new IndexRequest(MASTER_KEY_INDEX).id(MASTER_KEY)
                        .source(ImmutableMap.of(MASTER_KEY, masterKey))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                    client.index(masterKeyIndexRequest, ActionListener.wrap(indexResponse -> {
                        // Set generated key to master
                        logger.info("Master key has been initialized successfully");
                        this.masterKey = masterKey;
                        listener.onResponse(true);
                    }, indexException -> {
                        logger.error("Failed to index master key", indexException);
                        listener.onFailure(indexException);
                    }));

                } else {
                    // Set existing key to master
                    logger.info("Master key has already been initialized");
                    final String masterKey = (String) getResponse.getSourceAsMap().get(MASTER_KEY);
                    this.masterKey = masterKey;
                    listener.onResponse(true);
                }
            }, getRequestException -> {
                logger.error("Failed to search for master key from master_key index", getRequestException);
                listener.onFailure(getRequestException);
            }));

        } catch (Exception e) {
            logger.error("Failed to retrieve master key from master_key index", e);
            listener.onFailure(e);
        }
    }

    /**
     * Retrieves master key from system index if not yet set
     */
    protected void initializeMasterKeyIfAbsent() {
        if (masterKey != null) {
            return;
        }

        if (!clusterService.state().metadata().hasIndex(MASTER_KEY_INDEX)) {
            throw new FlowFrameworkException("Master Key Index has not been initialized", RestStatus.INTERNAL_SERVER_ERROR);
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                GetRequest getRequest = new GetRequest(MASTER_KEY_INDEX).id(MASTER_KEY);
                client.get(getRequest, ActionListener.wrap(response -> {
                    if (response.isExists()) {
                        this.masterKey = (String) response.getSourceAsMap().get(MASTER_KEY);
                    } else {
                        throw new FlowFrameworkException("Encryption key has not been initialized", RestStatus.NOT_FOUND);
                    }
                }, exception -> { throw new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)); }));
            }
        }
    }

}
