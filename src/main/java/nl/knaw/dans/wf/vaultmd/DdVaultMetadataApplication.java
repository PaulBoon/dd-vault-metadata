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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import nl.knaw.dans.wf.vaultmd.core.DataverseServiceImpl;
import nl.knaw.dans.wf.vaultmd.core.IdMintingServiceImpl;
import nl.knaw.dans.wf.vaultmd.core.IdValidatorImpl;
import nl.knaw.dans.wf.vaultmd.health.DataverseResponsiveCheck;
import nl.knaw.dans.wf.vaultmd.resources.StepInvocationResource;
import nl.knaw.dans.wf.vaultmd.resources.StepRollbackResource;

import java.util.concurrent.ExecutorService;

public class DdVaultMetadataApplication extends Application<DdVaultMetadataConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DdVaultMetadataApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Vault Metadata";
    }

    @Override
    public void initialize(final Bootstrap<DdVaultMetadataConfiguration> bootstrap) {
        bootstrap.getObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public void run(final DdVaultMetadataConfiguration configuration, final Environment environment) {
        final var dv = configuration.getDataverse().build();
        final var mdkey = configuration.getVaultMetadataKey().build();
        final var dataverseService = new DataverseServiceImpl(dv, mdkey);
        final var idValidator = new IdValidatorImpl();
        final var idMintingService = new IdMintingServiceImpl();

        environment.healthChecks().register("Dataverse", new DataverseResponsiveCheck(dv));
        ExecutorService executor = configuration.getTaskQueue().build(environment);
        environment.jersey().register(new StepInvocationResource(executor, dataverseService, idMintingService, idValidator));
        environment.jersey().register(new StepRollbackResource(executor));
    }

}
