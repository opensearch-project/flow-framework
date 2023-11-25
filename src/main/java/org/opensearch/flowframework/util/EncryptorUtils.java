/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
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
    public void setMasterKey(String masterKey) {
        this.masterKey = masterKey;
    }

    /**
     * Returns the master key
     * @return the master key
     */
    public String getMasterKey() {
        return this.masterKey;
    }

    /**
     * Randomly generate a master key
     * @return the master key string
     */
    public String generateMasterKey() {
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
        Template.Builder encryptedTemplateBuilder = new Template.Builder();

        Map<String, Workflow> encryptedWorkflows = new HashMap<>();
        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (!node.userInputs().containsKey(CREDENTIAL_FIELD)) {
                    processedNodes.add(node);
                } else {
                    // Encrypt all values within credential field
                    Map<String, String> credentials = (Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD);
                    credentials.replaceAll((key, cred) -> encrypt(cred));

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
                }
            }

            // Add encrypted workflow nodes to encrypted workflows
            encryptedWorkflows.put(entry.getKey(), new Workflow(entry.getValue().userParams(), processedNodes, entry.getValue().edges()));
        }

        Template encryptedTemplate = encryptedTemplateBuilder.name(template.name())
            .description(template.description())
            .useCase(template.useCase())
            .templateVersion(template.templateVersion())
            .compatibilityVersion(template.compatibilityVersion())
            .workflows(encryptedWorkflows)
            .uiMetadata(template.getUiMetadata())
            .user(template.getUser())
            .build();

        return encryptedTemplate;
    }

    /**
     * Decrypts template credentials
     * @param template the template to decrypt
     * @return template with decrypted credentials
     */
    public Template decryptTemplateCredentials(Template template) {
        Template.Builder decryptedTemplateBuilder = new Template.Builder();

        Map<String, Workflow> decryptedWorkflows = new HashMap<>();
        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (!node.userInputs().containsKey(CREDENTIAL_FIELD)) {
                    processedNodes.add(node);
                } else {
                    // Decrypt all values within credential field
                    Map<String, String> credentials = (Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD);
                    credentials.replaceAll((key, cred) -> decrypt(cred));

                    // Replace credentials field in node user inputs
                    Map<String, Object> decryptedUserInputs = new HashMap<>();
                    decryptedUserInputs.putAll(node.userInputs());
                    decryptedUserInputs.replace(CREDENTIAL_FIELD, credentials);

                    // build new node to add to processed nodes
                    WorkflowNode encryptedWorkflowNode = new WorkflowNode(
                        node.id(),
                        node.type(),
                        node.previousNodeInputs(),
                        decryptedUserInputs
                    );
                    processedNodes.add(encryptedWorkflowNode);
                }
            }

            // Add decrypted workflow nodes to decrypted workflows
            decryptedWorkflows.put(entry.getKey(), new Workflow(entry.getValue().userParams(), processedNodes, entry.getValue().edges()));
        }

        Template decryptedTemplate = decryptedTemplateBuilder.name(template.name())
            .description(template.description())
            .useCase(template.useCase())
            .templateVersion(template.templateVersion())
            .compatibilityVersion(template.compatibilityVersion())
            .workflows(decryptedWorkflows)
            .uiMetadata(template.getUiMetadata())
            .user(template.getUser())
            .build();

        return decryptedTemplate;
    }

    /**
     * Encrypts the given credential
     * @param credential the credential to encrypt
     * @return the encrypted credential
     */
    public String encrypt(String credential) {
        initializeMasterKey();
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
    public String decrypt(String encryptedCredential) {
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
     * Retrieves master key from system index if not yet initialized
     */
    public void initializeMasterKey() {
        if (masterKey != null) {
            return;
        }

        if (!clusterService.state().metadata().hasIndex(MASTER_KEY_INDEX)) {
            throw new FlowFrameworkException("Master Key Index has not been initialized", RestStatus.NOT_FOUND);
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
