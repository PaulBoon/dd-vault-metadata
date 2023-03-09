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
package nl.knaw.dans.wf.vaultmd;

import io.dropwizard.Configuration;
import nl.knaw.dans.lib.util.DataverseClientFactory;
import nl.knaw.dans.lib.util.ExecutorServiceFactory;
import nl.knaw.dans.wf.vaultmd.core.VaultMetadataKeyFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DdVaultMetadataConfiguration extends Configuration {

    @Valid
    @NotNull
    private ExecutorServiceFactory taskQueue;

    @Valid
    @NotNull
    private DataverseClientFactory dataverse;

    @Valid
    @NotNull
    private VaultMetadataKeyFactory vaultMetadataKey;
    
    public void setTaskQueue(ExecutorServiceFactory taskExecutorThreadPool) {
        this.taskQueue = taskExecutorThreadPool;
    }

    public ExecutorServiceFactory getTaskQueue() {
        return taskQueue;
    }

    public DataverseClientFactory getDataverse() {
        return dataverse;
    }

    public void setDataverse(DataverseClientFactory dataverse) {
        this.dataverse = dataverse;
    }

    public VaultMetadataKeyFactory getVaultMetadataKey() {
        return vaultMetadataKey;
    }

    public void setVaultMetadataKey(VaultMetadataKeyFactory vaultMetadataKey) {
        this.vaultMetadataKey = vaultMetadataKey;
    }
}
