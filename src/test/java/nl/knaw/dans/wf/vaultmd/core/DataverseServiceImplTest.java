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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataverseServiceImplTest {

    @Test
    void getAllReleasedOrDeaccessionedVersion_should_return_versions_in_descending_order() throws Exception {
        var service = Mockito.spy(new DataverseServiceImpl(Mockito.mock(DataverseClient.class),Mockito.mock(VaultMetadataKey.class)));
        var step = new StepInvocation("invocationId", "globalId", "datasetId", "1", "5");

        var version1 = TestUtilities.createDatasetVersion("bagId1", "nbn", 1, 1, "RELEASED");
        var version2 = TestUtilities.createDatasetVersion("bagId2", "nbn", 1, 2, "RELEASED");
        var version3 = TestUtilities.createDatasetVersion("bagId3", "nbn", 1, 3, "RELEASED");

        Mockito.doReturn(List.of(version1, version3, version2))
            .when(service).getAllDatasetVersions(Mockito.any());

        var result = new ArrayList<>(service.getAllReleasedOrDeaccessionedVersion(step));

        assertEquals(3, result.get(0).getVersionMinorNumber());
        assertEquals(2, result.get(1).getVersionMinorNumber());
        assertEquals(1, result.get(2).getVersionMinorNumber());
    }

}