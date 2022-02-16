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

import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.wf.vaultmd.api.StepInvocation;
import nl.knaw.dans.wf.vaultmd.legacy.SetVaultMetadataTaskScala;
import nl.knaw.dans.wf.vaultmd.legacy.WorkflowVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetVaultMetadataTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SetVaultMetadataTask.class);
    private final StepInvocation stepInvocation;
    private final DataverseClient dataverseClient;

    public SetVaultMetadataTask(StepInvocation stepInvocation, DataverseClient dataverseClient) {
        this.stepInvocation = stepInvocation;
        this.dataverseClient = dataverseClient;
    }

    @Override
    public String toString() {
        return "SetVaultMetadataTask{" + "invocationId='" + stepInvocation.getInvocationId() + "'}";
    }

    @Override
    public void run() {
        log.info("Running task " + this);
        new SetVaultMetadataTaskScala(
            WorkflowVariables.apply(stepInvocation.getInvocationId(), stepInvocation.getGlobalId(), stepInvocation.getDatasetId(), stepInvocation.getMajorVersion(), stepInvocation.getMinorVersion()),
            dataverseClient, "nl:ui:13-", 10, 1000).run();
    }
}
