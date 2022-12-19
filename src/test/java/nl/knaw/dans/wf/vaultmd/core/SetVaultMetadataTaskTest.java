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
import nl.knaw.dans.wf.vaultmd.api.StepInvocation;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

class SetVaultMetadataTaskTest {

    private final DataverseService dataverseServiceMock = Mockito.mock(DataverseService.class);

    private final IdMintingService mintingServiceMock = Mockito.spy(new IdMintingServiceImpl());
    private final IdValidator idValidator = new IdValidatorImpl();

    /*
     * Helper functions
     */
    private static DatasetVersion createDatasetVersion(String bagId, String nbn, int version, int versionMinor, String versionState) {
        var datasetVersion = new DatasetVersion();
        datasetVersion.setVersionNumber(version);
        datasetVersion.setVersionMinorNumber(versionMinor);
        datasetVersion.setVersionState(versionState);
        datasetVersion.setMetadataBlocks(new HashMap<>());

        var block = new MetadataBlock();
        block.setFields(new ArrayList<>());

        if (bagId != null) {
            block.getFields().add(new PrimitiveSingleValueField("dansBagId", bagId));
        }

        if (nbn != null) {
            block.getFields().add(new PrimitiveSingleValueField("dansNbn", nbn));
        }

        block.getFields().add(new PrimitiveSingleValueField("dansDataversePid", "globalId"));

        datasetVersion.getMetadataBlocks().put("dansDataVaultMetadata", block);

        return datasetVersion;
    }

    private static DatasetVersion createDatasetVersionWithoutVaultMetadataBlock(int version, int versionMinor, String versionState) {
        var datasetVersion = new DatasetVersion();
        datasetVersion.setVersionNumber(version);
        datasetVersion.setVersionMinorNumber(versionMinor);
        datasetVersion.setVersionState(versionState);
        datasetVersion.setMetadataBlocks(new HashMap<>());

        var block = new MetadataBlock();
        block.setFields(new ArrayList<>());

        return datasetVersion;
    }

    private static AbstractStringAssert<?> assertThatMetadataField(FieldList fieldList, String property) {
        return assertThat(fieldList.getFields())
            .filteredOn("typeName", property)
            .extracting("value")
            .first(as(InstanceOfAssertFactories.STRING));
    }

    @BeforeEach
    public void beforeEach() {
        Mockito.reset(dataverseServiceMock);
        Mockito.reset(mintingServiceMock);
    }

    SetVaultMetadataTask createTask(StepInvocation step) {
        return new SetVaultMetadataTask(step, dataverseServiceMock, mintingServiceMock, idValidator);
    }

    @Test
    void run() throws IOException, DataverseException {
        final var previousBagId = "urn:uuid:530dc968-4430-4186-bf58-08d98d717889";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(previousBagId, nbn, 1, 1, "DRAFT");
        var previous = createDatasetVersion(previousBagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        task.runTask();
        Mockito.verify(dataverseServiceMock).resumeWorkflow(eq(step), argThat(r -> r.getStatus().equals("Success")));
    }

    @Test
    void getVaultMetadata_with_inherited_bagId_and_nbn() throws IOException, DataverseException {
        final var previousBagId = "urn:uuid:530dc968-4430-4186-bf58-08d98d717889";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(previousBagId, nbn, 1, 1, "DRAFT");
        var previous = createDatasetVersion(previousBagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));
        Mockito.when(mintingServiceMock.mintBagId()).thenReturn(newBagId);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.1");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(nbn);
    }

    @Test
    void getVaultMetadata_without_previous_version_and_no_vault_metadata() throws IOException, DataverseException {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newNbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersionWithoutVaultMetadataBlock(1, 0, "DRAFT");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());
        Mockito.when(mintingServiceMock.mintBagId()).thenReturn(newBagId);
        Mockito.when(mintingServiceMock.mintUrnNbn()).thenReturn(newNbn);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "0");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.0");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(newNbn);
    }

    @Test
    void getVaultMetadata_with_different_previous_bagId() throws IOException, DataverseException {
        final var previousBagId = "urn:uuid:530dc968-4430-4186-bf58-08d98d717889";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(newBagId, nbn, 1, 1, "DRAFT");
        var previous = createDatasetVersion(previousBagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.1");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(nbn);
    }

    @Test
    void getVaultMetadata_without_previous_version_and_null_bagId_and_nbn() throws IOException, DataverseException {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newNbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(null, null, 1, 0, "DRAFT");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());
        Mockito.when(mintingServiceMock.mintBagId()).thenReturn(newBagId);
        Mockito.when(mintingServiceMock.mintUrnNbn()).thenReturn(newNbn);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "0");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.0");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(newNbn);
    }

    @Test
    void getVaultMetadata_without_previous_version_and_empty_string_bagId_and_nbn() throws IOException, DataverseException {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newNbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion("", "", 1, 0, "DRAFT");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());
        Mockito.when(mintingServiceMock.mintBagId()).thenReturn(newBagId);
        Mockito.when(mintingServiceMock.mintUrnNbn()).thenReturn(newNbn);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "0");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.0");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(newNbn);
    }

    @Test
    void getVaultMetadata_with_previous_version_and_null_bagId_and_nbn() throws IOException, DataverseException {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(null, null, 1, 1, "DRAFT");
        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));
        Mockito.when(mintingServiceMock.mintBagId())
            .thenReturn(newBagId);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        // the draft does not have a bag ID, but the previous version has, so it should mint a new bag ID
        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.1");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(nbn);
    }

    @Test
    void getVaultMetadata_with_previous_multiple_versions_should_mint_new_bagId() throws IOException, DataverseException {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var deaccessionedBagId = "urn:uuid:6fcfe06e-7f24-4989-9a27-18317c1970bb";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var draft = createDatasetVersion(bagId, null, 1, 1, "DRAFT");
        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");
        var deaccessioned = createDatasetVersion(deaccessionedBagId, nbn, 1, 0, "DEACCESSIONED");

        Mockito.when(dataverseServiceMock.getVersion(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(draft));
        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(deaccessioned, previous));
        Mockito.when(mintingServiceMock.mintBagId())
            .thenReturn(newBagId);

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        var metadata = task.getVaultMetadata(step);

        // the draft has the same ID as the previously published version, but different from the latest version which is deaccessioned
        // see https://drivenbydata.atlassian.net/browse/DD-1211 for more details
        assertThatMetadataField(metadata, "dansDataversePid").isEqualTo("globalId");
        assertThatMetadataField(metadata, "dansDataversePidVersion").isEqualTo("1.1");
        assertThatMetadataField(metadata, "dansBagId").isEqualTo(newBagId);
        assertThatMetadataField(metadata, "dansNbn").isEqualTo(nbn);
    }

    @Test
    void validateBagMetadata_should_not_throw_with_valid_input() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        assertDoesNotThrow(() -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", "1.0")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalArgumentException_with_bagid_is_null() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        assertThrows(IllegalArgumentException.class,
            () -> task.validateBagMetadata(step, createFieldList(null, nbn, "globalId", "1.0")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalArgumentException_with_nbn_is_null() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        assertThrows(IllegalArgumentException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, null, "globalId", "1.0")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalArgumentException_with_pid_is_null() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);
        assertThrows(IllegalArgumentException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, null, "1.0")));
    }

    @Test
    void validateBagMetadata_should_not_throw_with_pid_null_if_version_is_1_0() throws Exception {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "0");
        var task = createTask(step);
        assertDoesNotThrow(() -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, null, "1.0")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalArgumentException_with_pidVersion_is_null() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);

        assertThrows(IllegalArgumentException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", null)));
    }

    @Test
    void validateBagMetadata_should_not_throw_with_pidVersion_null_if_version_is_1_0() throws Exception {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "0");
        var task = createTask(step);

        assertDoesNotThrow(() -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", null)));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalArgumentException_when_no_older_versions_are_found() throws Exception {
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of());

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);

        assertThrows(IllegalArgumentException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", "1.1")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalStateException_when_older_version_does_not_have_required_fields() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";

        var previous = createDatasetVersion(bagId, null, 1, 0, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "1");
        var task = createTask(step);

        assertThrows(IllegalStateException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", "1.1")));
    }

    @Test
    void validateBagMetadata_should_throw_IllegalStateException_when_older_version_mismatch_on_nbn() throws Exception {
        final var bagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var newBagId = "urn:uuid:cbdf4d18-65af-42d2-baf3-6ca07ddfd3b2";
        final var nbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcae";
        final var otherNbn = "urn:nbn:nl:ui:13-73750978-5587-4e2b-937f-6b190e44fcad"; // last letter is different

        var previous1 = createDatasetVersion(bagId, nbn, 1, 0, "RELEASED");
        var previous2 = createDatasetVersion(bagId, otherNbn, 1, 1, "RELEASED");

        Mockito.when(dataverseServiceMock.getAllReleasedOrDeaccessionedVersion(Mockito.any()))
            .thenReturn(List.of(previous1, previous2));

        var step = new StepInvocation("invokeId", "globalId", "datasetId", "1", "2");
        var task = createTask(step);

        assertThrows(IllegalStateException.class,
            () -> task.validateBagMetadata(step, createFieldList(newBagId, nbn, "globalId", "1.2")));
    }

    FieldList createFieldList(String bagId, String nbn, String pid, String version) {
        var fields = Stream.of(
            new PrimitiveSingleValueField(SetVaultMetadataTask.DANS_BAG_ID, bagId),
            new PrimitiveSingleValueField(SetVaultMetadataTask.DANS_NBN, nbn),
            new PrimitiveSingleValueField(SetVaultMetadataTask.DANS_DATAVERSE_PID, pid),
            new PrimitiveSingleValueField(SetVaultMetadataTask.DANS_DATAVERSE_PID_VERSION, version)
        ).map(i -> (MetadataField) i).collect(Collectors.toList());

        var result = new FieldList();
        result.setFields(fields);
        return result;
    }
}
