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
package nl.knaw.dans.wf.vaultmd.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataverseResponsiveCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(DataverseResponsiveCheck.class);

    private final DataverseClient dataverseClient;

    public DataverseResponsiveCheck(DataverseClient dataverseClient) {
        this.dataverseClient = dataverseClient;
    }

    @Override
    protected Result check() {
        try {
            log.info("Checking if root dataverse can be reached...");
            dataverseClient.dataverse("root").view();
            log.info("OK: root dataverse is reachable.");
        }
        catch (IOException | DataverseException e) {
            log.warn("Dataverse connection check failed", e);
            return Result.unhealthy("Dataverse could not be reached: " + e.getMessage(), e);
        }
        return Result.healthy();
    }
}
