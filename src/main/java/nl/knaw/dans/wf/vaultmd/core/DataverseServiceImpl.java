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

import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.workflow.ResumeMessage;
import nl.knaw.dans.wf.vaultmd.api.StepInvocation;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;

public class DataverseServiceImpl implements DataverseService {
    private static final Logger log = LoggerFactory.getLogger(DataverseServiceImpl.class);
    private final DataverseClient dataverseClient;
    private final VersionComparator versionComparator = new VersionComparator();
    private static final String MDKEY_NAME = "dansDataVaultMetadata"; // the name of the metadata block
    private final String vaultMetadataKey;
    
    public DataverseServiceImpl(DataverseClient dataverseClient, String vaultMetadataKey) {
        this.dataverseClient = dataverseClient;
        this.vaultMetadataKey = vaultMetadataKey;
    }

    @Override
    public DataverseHttpResponse<Object> resumeWorkflow(StepInvocation stepInvocation, ResumeMessage resumeMessage) throws DataverseException, IOException {
        return dataverseClient.workflows().resume(stepInvocation.getInvocationId(), resumeMessage);
    }

    @Override
    public Optional<DatasetVersion> getVersion(StepInvocation stepInvocation, String name) throws IOException {
        try {
            return Optional.ofNullable(getDataset(stepInvocation).getVersion(name).getData());
        }
        catch (DataverseException e) {
            if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
                return Optional.empty();
            }
            else {
                // Don't return empty if there is some other error than "not found", to avoid confusing error messages.
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Collection<DatasetVersion> getAllReleasedOrDeaccessionedVersion(StepInvocation stepInvocation) throws DataverseException, IOException {
        return getAllDatasetVersions(stepInvocation).stream()
            .filter(d -> Set.of("RELEASED", "DEACCESSIONED").contains(d.getVersionState()))
            .sorted((a, b) -> -1 * versionComparator.compare(a, b))
            .collect(Collectors.toList());
    }

    @Override
    public void lockDataset(StepInvocation stepInvocation, String workflow) throws DataverseException, IOException {
        getDataset(stepInvocation).awaitLock("Workflow");
    }

    @Override
    public void editMetadata(StepInvocation stepInvocation, FieldList fieldList) throws DataverseException, IOException {
        if (vaultMetadataKey != null && !vaultMetadataKey.isBlank()) {
            log.debug("Using the VaultMetadataKey (name, value): {}, {}", MDKEY_NAME, vaultMetadataKey);
            var keyMap = new HashMap<String, String>(singletonMap(MDKEY_NAME, vaultMetadataKey));
            getDataset(stepInvocation).editMetadata(fieldList, true, keyMap);
        } else {
            log.debug("Not using the VaultMetadataKey");
            getDataset(stepInvocation).editMetadata(fieldList, true);
        }
    }

    DatasetApi getDataset(StepInvocation stepInvocation) {
        return dataverseClient.dataset(stepInvocation.getGlobalId(), stepInvocation.getInvocationId());
    }

    Collection<DatasetVersion> getAllDatasetVersions(StepInvocation stepInvocation) throws IOException, DataverseException {
        return getDataset(stepInvocation).getAllVersions().getData();
    }
}
