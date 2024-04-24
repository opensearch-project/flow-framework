/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Config;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;

import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.CONFIG_INDEX;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY;

/**
 * Encryption utility class
 */
public class EncryptorUtils {

    private static final Logger logger = LogManager.getLogger(EncryptorUtils.class);

    private static final String ALGORITHM = "AES";
    private static final String PROVIDER = "Custom";
    // Intentionally uppercase to work around localization bug
    // https://github.com/aws/aws-encryption-sdk-java/issues/1879
    private static final String WRAPPING_ALGORITHM = "AES/GCM/NOPADDING";

    private final ClusterService clusterService;
    private final Client client;
    private String masterKey;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new EncryptorUtils object
     * @param clusterService the cluster service
     * @param client the node client
     * @param xContentRegistry the OpenSearch XContent Registry
     */
    public EncryptorUtils(ClusterService clusterService, Client client, NamedXContentRegistry xContentRegistry) {
        this.masterKey = null;
        this.clusterService = clusterService;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Sets the master key
     * @param masterKey the master key
     */
    void setMasterKey(String masterKey) {
        this.masterKey = masterKey;
    }

    /**
     * Returns the master key
     * @return the master key
     */
    String getMasterKey() {
        return this.masterKey;
    }

    /**
     * Randomly generate a master key
     * @return the master key string
     */
    String generateMasterKey() {
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

    // TODO : Improve processTemplateCredentials to encrypt different fields based on the WorkflowStep type
    /**
     * Applies the given cipher function on template credentials
     * @param template the template to process
     * @param cipherFunction the encryption/decryption function to apply on credential values
     * @return template with encrypted credentials
     */
    private Template processTemplateCredentials(Template template, Function<String, String> cipherFunction) {
        Map<String, Workflow> processedWorkflows = new HashMap<>();
        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (node.userInputs().containsKey(CREDENTIAL_FIELD)) {
                    // Apply the cipher funcion on all values within credential field
                    @SuppressWarnings("unchecked")
                    Map<String, String> credentials = new HashMap<>((Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD));
                    credentials.replaceAll((key, cred) -> cipherFunction.apply(cred));

                    // Replace credentials field in node user inputs
                    Map<String, Object> processedUserInputs = new HashMap<>();
                    processedUserInputs.putAll(node.userInputs());
                    processedUserInputs.replace(CREDENTIAL_FIELD, credentials);

                    // build new node to add to processed nodes
                    WorkflowNode processedWorkflowNode = new WorkflowNode(
                        node.id(),
                        node.type(),
                        node.previousNodeInputs(),
                        processedUserInputs
                    );
                    processedNodes.add(processedWorkflowNode);
                } else {
                    processedNodes.add(node);
                }
            }

            // Add processed workflow nodes to processed workflows
            processedWorkflows.put(entry.getKey(), new Workflow(entry.getValue().userParams(), processedNodes, entry.getValue().edges()));
        }

        return new Template.Builder(template).workflows(processedWorkflows).build();
    }

    /**
     * Encrypts the given credential
     * @param credential the credential to encrypt
     * @return the encrypted credential
     */
    String encrypt(final String credential) {
        initializeMasterKeyIfAbsent();
        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        byte[] bytes = Base64.getDecoder().decode(masterKey);
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(new SecretKeySpec(bytes, ALGORITHM), PROVIDER, "", WRAPPING_ALGORITHM);
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
    String decrypt(final String encryptedCredential) {
        initializeMasterKeyIfAbsent();
        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();

        byte[] bytes = Base64.getDecoder().decode(masterKey);
        JceMasterKey jceMasterKey = JceMasterKey.getInstance(new SecretKeySpec(bytes, ALGORITHM), PROVIDER, "", WRAPPING_ALGORITHM);

        final CryptoResult<byte[], JceMasterKey> decryptedResult = crypto.decryptData(
            jceMasterKey,
            Base64.getDecoder().decode(encryptedCredential)
        );
        return new String(decryptedResult.getResult(), StandardCharsets.UTF_8);
    }

    // TODO : Improve redactTemplateCredentials to redact different fields
    /**
     * Removes the credential fields from a template
     * @param user User
     * @param template the template
     * @return the redacted template
     */
    public Template redactTemplateSecuredFields(User user, Template template) {
        Map<String, Workflow> processedWorkflows = new HashMap<>();

        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (node.userInputs().containsKey(CREDENTIAL_FIELD)) {

                    // Remove credential field field in node user inputs
                    Map<String, Object> processedUserInputs = new HashMap<>(node.userInputs());
                    processedUserInputs.remove(CREDENTIAL_FIELD);

                    // build new node to add to processed nodes
                    processedNodes.add(new WorkflowNode(node.id(), node.type(), node.previousNodeInputs(), processedUserInputs));
                } else {
                    processedNodes.add(node);
                }
            }

            // Add processed workflow nodes to processed workflows
            processedWorkflows.put(entry.getKey(), new Workflow(entry.getValue().userParams(), processedNodes, entry.getValue().edges()));
        }

        if (ParseUtils.isAdmin(user)) {
            return new Template.Builder(template).workflows(processedWorkflows).build();
        }

        return new Template.Builder(template).user(null).workflows(processedWorkflows).build();
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
            // Using the master_key string as the document id
            GetRequest getRequest = new GetRequest(CONFIG_INDEX).id(MASTER_KEY);
            client.get(getRequest, ActionListener.wrap(getResponse -> {

                if (!getResponse.isExists()) {
                    Config config = new Config(generateMasterKey(), Instant.now());
                    IndexRequest masterKeyIndexRequest = new IndexRequest(CONFIG_INDEX).id(MASTER_KEY)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        masterKeyIndexRequest.source(config.toXContent(builder, ToXContent.EMPTY_PARAMS));
                    }
                    client.index(masterKeyIndexRequest, ActionListener.wrap(indexResponse -> {
                        // Set generated key to master
                        logger.info("Config has been initialized successfully");
                        this.masterKey = config.masterKey();
                        listener.onResponse(true);
                    }, indexException -> {
                        logger.error("Failed to index config", indexException);
                        listener.onFailure(indexException);
                    }));

                } else {
                    // Set existing key to master
                    logger.debug("Config has already been initialized, fetching key");
                    try (
                        XContentParser parser = ParseUtils.createXContentParserFromRegistry(
                            xContentRegistry,
                            getResponse.getSourceAsBytesRef()
                        )
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        Config config = Config.parse(parser);
                        this.masterKey = config.masterKey();
                        listener.onResponse(true);
                    } catch (FlowFrameworkException e) {
                        listener.onFailure(e);
                    }
                }
            }, getRequestException -> {
                logger.error("Failed to search for config from config index", getRequestException);
                listener.onFailure(getRequestException);
            }));

        } catch (Exception e) {
            logger.error("Failed to retrieve config from config index", e);
            listener.onFailure(e);
        }
    }

    /**
     * Retrieves master key from system index if not yet set
     */
    void initializeMasterKeyIfAbsent() {
        if (masterKey != null) {
            return;
        }

        if (!clusterService.state().metadata().hasIndex(CONFIG_INDEX)) {
            throw new FlowFrameworkException("Config Index has not been initialized", RestStatus.INTERNAL_SERVER_ERROR);
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                GetRequest getRequest = new GetRequest(CONFIG_INDEX).id(MASTER_KEY);
                client.get(getRequest, ActionListener.wrap(response -> {
                    if (response.isExists()) {
                        try (
                            XContentParser parser = ParseUtils.createXContentParserFromRegistry(
                                xContentRegistry,
                                response.getSourceAsBytesRef()
                            )
                        ) {
                            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                            Config config = Config.parse(parser);
                            this.masterKey = config.masterKey();
                        }
                    } else {
                        throw new FlowFrameworkException("Master key has not been initialized in config index", RestStatus.NOT_FOUND);
                    }
                },
                    exception -> {
                        throw new FlowFrameworkException("Failed to get master key from config index", ExceptionsHelper.status(exception));
                    }
                ));
            }
        }
    }

}
