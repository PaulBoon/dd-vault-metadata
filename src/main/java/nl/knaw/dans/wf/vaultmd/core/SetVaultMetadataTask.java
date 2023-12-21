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
import java.util.Set;
import java.util.stream.Collectors;

public class SetVaultMetadataTask implements Runnable {
    public static final String DANS_NBN = "dansNbn";
    public static final String DANS_BAG_ID = "dansBagId";
    public static final String DANS_DATAVERSE_PID = "dansDataversePid";
    public static final String DANS_DATAVERSE_PID_VERSION = "dansDataversePidVersion";
    private static final Logger log = LoggerFactory.getLogger(SetVaultMetadataTask.class);
    private static final int MAX_RETRIES = 10;
    private static final int RETRY_DELAY_MS = 1000;

    private final DataverseService dataverseService;
    private final StepInvocation stepInvocation;

    private final IdMintingService mintingService;
    private final IdValidator idValidator;

    public SetVaultMetadataTask(StepInvocation stepInvocation, DataverseService dataverseService, IdMintingService mintingService, IdValidator idValidator) {
        this.stepInvocation = stepInvocation;
        this.dataverseService = dataverseService;
        this.mintingService = mintingService;
        this.idValidator = idValidator;
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

            log.info("Validating metadata for dataset {}", stepInvocation.getGlobalId());
            validateBagMetadata(stepInvocation, metadata);

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

        var allVersions = dataverseService.getAllReleasedOrDeaccessionedVersion(stepInvocation);

        // get a list of all published or deaccessioned dataset versions
        var bagIds = allVersions.stream()
            .map(datasetVersion -> getVaultMetadataFieldValue(datasetVersion, DANS_BAG_ID))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

        // if the latest version exists, use that to get the bag id
        var bagId = getBagId(draftVersion, bagIds);

        // find the latest version
        var latestVersion = allVersions.stream().findFirst();

        // if the latest version exists, use that to get the NBN
        var nbn = latestVersion.map(this::getNbn)
            .orElseGet(() -> getVaultMetadataFieldValue(draftVersion, DANS_NBN).orElseGet(mintingService::mintUrnNbn));

        var version = String.format("%s.%s", stepInvocation.getMajorVersion(), stepInvocation.getMinorVersion());

        log.debug("Generating metadata with values dansDataversePid={}, dansDataversePidVersion={}, {}={}, {}={}",
            stepInvocation.getGlobalId(), version, DANS_BAG_ID, bagId, DANS_NBN, nbn);

        var fieldList = new FieldList();
        fieldList.add(new PrimitiveSingleValueField(DANS_DATAVERSE_PID, stepInvocation.getGlobalId()));
        fieldList.add(new PrimitiveSingleValueField(DANS_DATAVERSE_PID_VERSION, version));
        fieldList.add(new PrimitiveSingleValueField(DANS_BAG_ID, bagId));
        fieldList.add(new PrimitiveSingleValueField(DANS_NBN, nbn));

        return fieldList;
    }

    /**
     * //@formatter:off
     * For versions > 1.0:
     *  - there MUST exist a previous released or deaccessioned version.
     *  - dansDataversePid, dansDataversePidVersion, dansBagId and dansNbn MUST always be filled in.
     *  - dansDataversePid and dansNbn must each have the same value for all versions
     * dansNbn must be an urn:nbn
     * dansBagId must be an urn:uuid
     *
     * @param stepInvocation
     * @param fieldList
     * @throws IOException
     * @throws DataverseException
     * @throws IllegalArgumentException when a validation error occurred
     * //@formatter:on
     */
    void validateBagMetadata(StepInvocation stepInvocation, FieldList fieldList) throws IOException, DataverseException {

        var bagId = getRequiredFieldListValue(fieldList, DANS_BAG_ID);
        var nbn = getRequiredFieldListValue(fieldList, DANS_NBN);

        log.debug("Validating bagId '{}' to be valid urn:uuid", bagId);
        if (!idValidator.isValidUrnUuid(bagId)) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid urn:uuid", bagId));
        }

        log.debug("Validating nbn '{}' to be valid urn:nbn", nbn);
        if (!idValidator.isValidUrnNbn(nbn)) {
            throw new IllegalArgumentException(String.format("'%s' is not a valid urn:nbn", nbn));
        }

        var majorVersion = Integer.parseInt(stepInvocation.getMajorVersion());
        var minorVersion = Integer.parseInt(stepInvocation.getMinorVersion());

        // anything greater than 1.0
        if (majorVersion > 1 || (majorVersion == 1 && minorVersion > 0)) {
            var pidVersion = getRequiredFieldListValue(fieldList, DANS_DATAVERSE_PID_VERSION);
            log.trace("Found '{}' property with value '{}'", DANS_DATAVERSE_PID_VERSION, pidVersion);
            var pid = getRequiredFieldListValue(fieldList, DANS_DATAVERSE_PID);
            log.trace("Found '{}' property with value '{}'", DANS_DATAVERSE_PID, pid);

            var allVersions = dataverseService.getAllReleasedOrDeaccessionedVersion(stepInvocation);

            // if there are no previous versions, it failed to validate
            if (allVersions.size() == 0) {
                throw new IllegalArgumentException(String.format(
                    "Version %s.%s is greater than 1.0, but no previous version found", majorVersion, minorVersion
                ));
            }

            // now ensure pid and nbn are the same for each version
            for (var version : allVersions) {
                var otherPid = getVaultMetadataFieldValue(version, DANS_DATAVERSE_PID)
                    .orElseThrow(() -> new IllegalStateException(String.format(
                        "Released or deaccessioned version found without '%s' property (version %s.%s)",
                        DANS_DATAVERSE_PID, version.getVersionNumber(), version.getVersionMinorNumber()
                    )));

                var otherNbn = getVaultMetadataFieldValue(version, DANS_NBN)
                    .orElseThrow(() -> new IllegalStateException(String.format(
                        "Released or deaccessioned version found without '%s' property (version %s.%s)",
                        DANS_NBN, version.getVersionNumber(), version.getVersionMinorNumber()
                    )));

                if (!StringUtils.equals(pid, otherPid)) {
                    throw new IllegalStateException(String.format(
                        "Mismatch in '%s' property, expected '%s' in version %s.%s, but instead found '%s'",
                        DANS_DATAVERSE_PID, pid, version.getVersionNumber(), version.getVersionMinorNumber(), otherPid
                    ));
                }

                if (!StringUtils.equals(nbn, otherNbn)) {
                    throw new IllegalStateException(String.format(
                        "Mismatch in '%s' property, expected '%s' in version %s.%s, but instead found '%s'",
                        DANS_NBN, pid, version.getVersionNumber(), version.getVersionMinorNumber(), otherPid
                    ));
                }
            }
        }
    }

    private String getRequiredFieldListValue(FieldList fieldList, String key) {
        return fieldList.getFields()
            .stream()
            .filter(i -> i.getTypeName().equals(key))
            .filter(i -> i instanceof PrimitiveSingleValueField)
            .map(i -> (PrimitiveSingleValueField) i)
            .map(PrimitiveSingleValueField::getValue)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("'%s' missing from metadata", key)));
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

                if (e.getStatus() == 404) {
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

    String getBagId(DatasetVersion draftVersion, Set<String> bagIds) {
        var draftBagId = getVaultMetadataFieldValue(draftVersion, DANS_BAG_ID);

        /*
         * create a new bag id if:
         * - the draft bag doesn't have a bag id
         * - any of the published or deaccessioned versions have the same bag ID as the draft already
         * - the latest published version does not exist, and the bag id in the draft is also empty
         */
        return draftBagId.map(bagId -> {
                /*
                 * This happens after publishing a new version via the UI. The bagId from the previous version is inherited by the new draft. However, we
                 * want every version to have a unique bagId.
                 */
                if (StringUtils.isBlank(bagId) || bagIds.contains(bagId)) {
                    return mintingService.mintBagId();
                }

                /*
                 * Provided by machine deposit.
                 */
                return bagId;
            })
            .orElseGet(mintingService::mintBagId);
    }
}
