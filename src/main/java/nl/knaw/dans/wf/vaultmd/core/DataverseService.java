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
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.workflow.ResumeMessage;
import nl.knaw.dans.wf.vaultmd.api.StepInvocation;

import java.io.IOException;
import java.util.Optional;

public interface DataverseService {

    DataverseHttpResponse<Object> resumeWorkflow(StepInvocation stepInvocation, ResumeMessage resumeMessage) throws DataverseException, IOException;

    Optional<DatasetVersion> getVersion(StepInvocation stepInvocation, String name) throws DataverseException, IOException;

    Optional<DatasetVersion> getLatestReleasedOrDeaccessionedVersion(StepInvocation stepInvocation) throws DataverseException, IOException;

    void lockDataset(StepInvocation stepInvocation, String workflow) throws DataverseException, IOException;

    void editMetadata(StepInvocation stepInvocation, FieldList fieldList) throws DataverseException, IOException;
}
