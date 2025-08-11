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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Config;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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

    // concurrent map can't have null as a key. This key is to support single tenancy
    public static final String DEFAULT_TENANT_ID = "";

    private final ClusterService clusterService;
    private final Client client;
    private final SdkClient sdkClient;
    private final Map<String, String> tenantMasterKeys;
    private final NamedXContentRegistry xContentRegistry;
    private static boolean multiTenancyEnabled;

    /**
     * Instantiates a new EncryptorUtils object
     * @param clusterService the cluster service
     * @param client the node client
     * @param sdkClient the Multitenant Client
     * @param xContentRegistry the OpenSearch XContent Registry
     * @param multiTenancyEnabled whether multitenancy is enabled
     */
    public EncryptorUtils(
        ClusterService clusterService,
        Client client,
        SdkClient sdkClient,
        NamedXContentRegistry xContentRegistry,
        boolean multiTenancyEnabled
    ) {
        this.tenantMasterKeys = new ConcurrentHashMap<>();
        this.clusterService = clusterService;
        this.client = client;
        this.sdkClient = sdkClient;
        this.xContentRegistry = xContentRegistry;
        this.multiTenancyEnabled = multiTenancyEnabled;
    }

    /**
     * Sets the master key
     * @param tenantId The tenant id. If null, sets the key for the default id.
     * @param masterKey the master key
     */
    void setMasterKey(@Nullable String tenantId, String masterKey) {
        this.tenantMasterKeys.put(Objects.requireNonNullElse(tenantId, DEFAULT_TENANT_ID), masterKey);
    }

    /**
     * Returns the master key
     * @param tenantId The tenant id. If null, gets the key for the default id.
     * @return the master key
     */
    String getMasterKey(@Nullable String tenantId) {
        return tenantMasterKeys.get(Objects.requireNonNullElse(tenantId, DEFAULT_TENANT_ID));
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
    private Template processTemplateCredentials(Template template, BiFunction<String, String, String> cipherFunction) {
        Map<String, Workflow> processedWorkflows = new HashMap<>();
        for (Map.Entry<String, Workflow> entry : template.workflows().entrySet()) {

            List<WorkflowNode> processedNodes = new ArrayList<>();
            for (WorkflowNode node : entry.getValue().nodes()) {
                if (node.userInputs().containsKey(CREDENTIAL_FIELD)) {
                    // Apply the cipher funcion on all values within credential field
                    @SuppressWarnings("unchecked")
                    Map<String, String> credentials = new HashMap<>((Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD));
                    credentials.replaceAll((key, cred) -> cipherFunction.apply(cred, template.getTenantId()));

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

        return Template.builder(template).workflows(processedWorkflows).build();
    }

    /**
     * Encrypts the given credential
     * @param credential the credential to encrypt
     * @param tenantId The tenant id. If null, encrypts for the default tenant id.
     * @return the encrypted credential
     */
    String encrypt(final String credential, @Nullable String tenantId) {
        CountDownLatch latch = new CountDownLatch(1);
        initializeMasterKeyIfAbsent(tenantId).whenComplete((v, throwable) -> latch.countDown());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new FlowFrameworkException("Timeout while initializing master key", RestStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlowFrameworkException("Interrupted while initializing master key", RestStatus.REQUEST_TIMEOUT);
        }

        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        byte[] bytes = Base64.getDecoder().decode(getMasterKey(tenantId));
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
     * @param tenantId The tenant id. If null, decrypts for the default tenant id.
     * @return the decrypted credential
     */
    String decrypt(final String encryptedCredential, @Nullable String tenantId) {
        CountDownLatch latch = new CountDownLatch(1);
        initializeMasterKeyIfAbsent(tenantId).whenComplete((v, throwable) -> latch.countDown());
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new FlowFrameworkException("Timeout while initializing master key", RestStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlowFrameworkException("Interrupted while initializing master key", RestStatus.REQUEST_TIMEOUT);
        }
        final AwsCrypto crypto = AwsCrypto.builder().withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt).build();
        byte[] bytes = Base64.getDecoder().decode(getMasterKey(tenantId));
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
            return Template.builder(template).workflows(processedWorkflows).build();
        }

        return Template.builder(template).user(null).workflows(processedWorkflows).build();
    }

    /**
     * Retrieves an existing master key or generates a new key to index
     * @param tenantId The tenant id. If null, initializes the key for the default tenant id.
     * @param listener the action listener
     */
    public void initializeMasterKey(@Nullable String tenantId, ActionListener<Boolean> listener) {
        // Config index has already been created or verified
        cacheMasterKeyFromConfigIndex(tenantId).thenApply(v -> {
            // Key exists and has been cached successfully
            listener.onResponse(true);
            return null;
        }).exceptionally(throwable -> {
            Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
            // The cacheMasterKey method only completes exceptionally with FFE
            if (exception instanceof FlowFrameworkException) {
                FlowFrameworkException ffe = (FlowFrameworkException) exception;
                if (ffe.status() == RestStatus.NOT_FOUND) {
                    // Key doesn't exist, need to generate and index a new one
                    generateAndIndexNewMasterKey(tenantId, listener);
                } else {
                    listener.onFailure(ffe);
                }
            } else {
                // Shouldn't get here
                listener.onFailure(exception);
            }
            return null;
        });
    }

    private void generateAndIndexNewMasterKey(String tenantId, ActionListener<Boolean> listener) {
        String masterKeyId = tenantId == null ? MASTER_KEY : MASTER_KEY + "_" + hashString(tenantId);
        Config config = new Config(generateMasterKey(), Instant.now());
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(CONFIG_INDEX)
            .id(masterKeyId)
            .tenantId(tenantId)
            .overwriteIfExists(false)
            .dataObject(config)
            .build();
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            sdkClient.putDataObjectAsync(putRequest).whenComplete((r, throwable) -> {
                context.restore();
                if (throwable == null) {
                    // Set generated key to master
                    logger.info("Config has been initialized successfully");
                    setMasterKey(tenantId, config.masterKey());
                    listener.onResponse(true);
                } else {
                    Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                    // In a race condition another thread may have already set the key
                    // In this case we ignore config.masterKey(), and fetch and cache existing key
                    if (exception instanceof OpenSearchStatusException
                        && ((OpenSearchStatusException) exception).status() == RestStatus.CONFLICT) {
                        logger.debug("Concurrent master key creation detected, retrying to get existing key");
                        cacheMasterKeyFromConfigIndex(tenantId).handle((v, ex) -> {
                            if (ex == null) {
                                listener.onResponse(true);
                            } else {
                                listener.onFailure(SdkClientUtils.unwrapAndConvertToException(ex));
                            }
                            return null;
                        });
                    } else {
                        logger.error("Failed to index new master key in config for tenant id {}", tenantId, exception);
                        listener.onFailure(exception);
                    }
                }
            });
        }
    }

    /**
     * Called by encrypt and decrypt functions to retrieve master key from tenantMasterKeys map if set. If not, checks config system index (which must exist), fetches key and puts in tenantMasterKeys map.
     * @param tenantId The tenant id. If null, initializes the key for the default id.
     * @return a future that will complete when the key is initialized (or throws an exception)
     */
    CompletableFuture<Void> initializeMasterKeyIfAbsent(@Nullable String tenantId) {
        // Happy case, key already in map
        if (this.tenantMasterKeys.containsKey(Objects.requireNonNullElse(tenantId, DEFAULT_TENANT_ID))) {
            return CompletableFuture.completedFuture(null);
        }
        // Key not in map
        if (!FlowFrameworkIndicesHandler.doesIndexExistMultitenant(clusterService, CONFIG_INDEX, multiTenancyEnabled)) {
            return CompletableFuture.failedFuture(
                new FlowFrameworkException("Config Index has not been initialized", RestStatus.INTERNAL_SERVER_ERROR)
            );
        }
        // Fetch from config index and store in map
        return cacheMasterKeyFromConfigIndex(tenantId);
    }

    private CompletableFuture<Void> cacheMasterKeyFromConfigIndex(String tenantId) {
        // This method assumes the config index must exist
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true);
            String masterKeyId = tenantId == null ? MASTER_KEY : MASTER_KEY + "_" + hashString(tenantId);
            sdkClient.getDataObjectAsync(
                GetDataObjectRequest.builder()
                    .index(CONFIG_INDEX)
                    .id(masterKeyId)
                    .tenantId(tenantId)
                    .fetchSourceContext(fetchSourceContext)
                    .build()
            ).whenComplete((r, throwable) -> {
                context.restore();
                if (throwable == null) {
                    try {
                        GetResponse response = r.parser() == null ? null : GetResponse.fromXContent(r.parser());
                        if (response != null && response.isExists()) {
                            try (
                                XContentParser parser = ParseUtils.createXContentParserFromRegistry(
                                    xContentRegistry,
                                    response.getSourceAsBytesRef()
                                )
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                Config config = Config.parse(parser);
                                setMasterKey(tenantId, config.masterKey());
                                resultFuture.complete(null);
                            }
                        } else {
                            resultFuture.completeExceptionally(
                                new FlowFrameworkException("Master key has not been initialized in config index", RestStatus.NOT_FOUND)
                            );
                        }
                    } catch (IOException e) {
                        logger.error("Failed to parse config index getResponse", e);
                        resultFuture.completeExceptionally(
                            new FlowFrameworkException("Failed to parse config index getResponse", RestStatus.INTERNAL_SERVER_ERROR)
                        );
                    }
                } else {
                    Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                    logger.error("Failed to get master key from config index", exception);
                    resultFuture.completeExceptionally(
                        new FlowFrameworkException("Failed to get master key from config index", ExceptionsHelper.status(exception))
                    );
                }
            });
            return resultFuture;
        }
    }

    private String hashString(String input) {
        try {
            // Create a MessageDigest instance for SHA-256
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // Perform the hashing and get the byte array
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));

            // Convert the byte array to a Base64 encoded string
            return Base64.getUrlEncoder().encodeToString(hashBytes);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error: Unable to compute hash", e);
        }
    }

}
