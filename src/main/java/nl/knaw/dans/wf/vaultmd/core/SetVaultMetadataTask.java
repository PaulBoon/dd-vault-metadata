/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.wf.vaultmd.core;

import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.workflow.ResumeMessage;
import nl.knaw.dans.wf.vaultmd.api.StepInvocation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SetVaultMetadataTask implements Runnable {
    public static final String DANS_NBN = "dansNbn";
    public static final String DANS_BAG_ID = "dansBagId";
    private static final Logger log = LoggerFactory.getLogger(SetVaultMetadataTask.class);
    private static final int MAX_RETRIES = 10;
    private static final int RETRY_DELAY_MS = 1000;

    private final DataverseService dataverseService;
    private final StepInvocation stepInvocation;

    private final IdMintingService mintingService;

    public SetVaultMetadataTask(StepInvocation stepInvocation, DataverseService dataverseService, IdMintingService mintingService) {
        this.stepInvocation = stepInvocation;
        this.dataverseService = dataverseService;
        this.mintingService = mintingService;
    }

    @Override
    public String toString() {
        return "SetVaultMetadataTask{" + "invocationId='" + stepInvocation.getInvocationId() + "'}";
    }

    @Override
    public void run() {
        log.info("Running task " + this);
        runTask();
        log.info("Completed running task " + this);
    }

    void runTask() {
        try {
            // lock dataset before doing work
            log.info("Locking dataset {}", stepInvocation.getGlobalId());
            dataverseService.lockDataset(stepInvocation, "Workflow");

            // update metadata
            var metadata = getVaultMetadata(stepInvocation);
            log.info("Updating metadata for dataset {}", stepInvocation.getGlobalId());
            dataverseService.editMetadata(stepInvocation, metadata);

            // resume workflow
            resumeWorkflow(stepInvocation);
            log.info("Vault metadata set for dataset {}. Dataset resume called.", stepInvocation.getGlobalId());
        }
        // catch all kinds of exceptions
        catch (Exception e) {
            log.error("SetVaultMetadataTask for dataset {} failed. Resuming dataset with 'fail=true'", stepInvocation.getGlobalId(), e);

            try {
                dataverseService.resumeWorkflow(stepInvocation,
                    new ResumeMessage("Failure", e.getMessage(), "Publication failed: pre-publication workflow returned an error"));
            }
            catch (IOException | DataverseException ex) {
                log.error("Error resuming workflow with Failure status", ex);
            }
        }
    }

    Optional<List<MetadataField>> getVaultMetadata(DatasetVersion datasetVersion) {
        return Optional.ofNullable(datasetVersion.getMetadataBlocks().get("dansDataVaultMetadata")).map(MetadataBlock::getFields);
    }

    Optional<String> getVaultMetadataFieldValue(DatasetVersion datasetVersion, String fieldName) {
        // this gets the single value of a field in the metadata, eg dansDataVaultMetadata.fields[1].value
        // where fields[1].typeName equals the fieldName parameter
        var result = getVaultMetadata(datasetVersion)
            .map(fields -> fields.stream()
                .filter(field -> field.getTypeName().equals(fieldName))
                .findFirst())
            .flatMap(i -> i)
            .map(f -> (PrimitiveSingleValueField) f)
            .map(PrimitiveSingleValueField::getValue);

        if (result.equals(Optional.of(""))) {
            return Optional.empty();
        }
        return result;
    }

    FieldList getVaultMetadata(StepInvocation stepInvocation) throws IOException, DataverseException {
        var draftVersion = dataverseService.getVersion(stepInvocation, ":draft")
            .orElseThrow(() -> new IllegalArgumentException("No draft version found"));

        var optLatestVersion = dataverseService.getLatestReleasedOrDeaccessionedVersion(stepInvocation);

        // if a latest version exists, use that to get the bag id
        var bagId = optLatestVersion.map(latestVersion -> getBagId(draftVersion, latestVersion))
            .orElseGet(() -> getBagId(draftVersion));

        // if a latest version exists, use that to get the NBN
        var nbn = optLatestVersion.map(this::getNbn)
            .orElseGet(() -> getVaultMetadataFieldValue(draftVersion, DANS_NBN).orElseGet(mintingService::mintUrnNbn));

        var version = String.format("%s.%s", stepInvocation.getMajorVersion(), stepInvocation.getMinorVersion());

        log.debug("Generating metadata with values dansDataversePid={}, dansDataversePidVersion={}, {}={}, {}={}",
            stepInvocation.getGlobalId(), version, DANS_BAG_ID, bagId, DANS_NBN, nbn);

        var fieldList = new FieldList();
        fieldList.add(new PrimitiveSingleValueField("dansDataversePid", stepInvocation.getGlobalId()));
        fieldList.add(new PrimitiveSingleValueField("dansDataversePidVersion", version));
        fieldList.add(new PrimitiveSingleValueField(DANS_BAG_ID, bagId));
        fieldList.add(new PrimitiveSingleValueField(DANS_NBN, nbn));

        return fieldList;
    }

    void resumeWorkflow(StepInvocation stepInvocation) throws IOException, DataverseException, InterruptedException {
        var tried = 0;

        DataverseException lastException = null;

        while (tried++ < MAX_RETRIES) {
            try {
                log.trace("Resuming workflow with id {}, attempt {}", stepInvocation.getGlobalId(), tried);
                dataverseService.resumeWorkflow(stepInvocation, new ResumeMessage("Success", "", ""));
                return;
            }
            catch (DataverseException e) {
                log.warn("Unable to resume workflow due to Dataverse error", e);

                if (e.getHttpResponse().getStatusLine().getStatusCode() == 404) {
                    // retrying
                    log.debug("Sleeping {} ms before next try", RETRY_DELAY_MS);
                    Thread.sleep(RETRY_DELAY_MS);
                    lastException = e;
                }
                else {
                    log.error("Workflow could not be resumed for dataset {}. Number of retries: {}. Time between retries in ms: {}", stepInvocation.getGlobalId(), tried,
                        RETRY_DELAY_MS);
                    throw e;
                }
            }
        }

        throw lastException;
    }

    String getNbn(DatasetVersion latestPublishedDataset) {
        // validate latest published version has a bag id
        return getVaultMetadataFieldValue(latestPublishedDataset, DANS_NBN)
            .orElseThrow(() -> new IllegalArgumentException("Dataset with a latest published version without NBN found!"));
    }

    String getBagId(DatasetVersion draftVersion) {
        var draftBagId = getVaultMetadataFieldValue(draftVersion, DANS_BAG_ID);

        return getVaultMetadataFieldValue(draftVersion, DANS_BAG_ID)
            .orElseGet(() -> draftBagId.orElse(mintingService.mintBagId()));
    }

    String getBagId(DatasetVersion draftVersion, DatasetVersion latestPublishedDataset) {
        var draftBagId = getVaultMetadataFieldValue(draftVersion, DANS_BAG_ID);

        /*
        create a new bag id if:
        - the draft bag doesn't have a bag id
        - the latest published bag id is the same as the draft bag id
        - the latest published version does not exist, and the bag id in the draft is also empty
         */
        return getVaultMetadataFieldValue(latestPublishedDataset, DANS_BAG_ID)
            .map(latestBagId -> {
                if (draftBagId.isEmpty() || latestBagId.equals(draftBagId.orElse(null))) {
                    /*
                     * This happens after publishing a new version via the UI. The bagId from the previous version is inherited by the new draft. However, we
                     * want every version to have a unique bagId.
                     */
                    return mintingService.mintBagId();
                }
                else {
                    /*
                     * Provided by machine deposit.
                     */
                    return draftBagId.get();
                }
            }).orElseThrow(() -> new IllegalArgumentException("Dataset with a latest published version without bag ID found!"));
    }

}
