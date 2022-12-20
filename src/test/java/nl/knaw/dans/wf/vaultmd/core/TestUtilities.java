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

import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;

import java.util.ArrayList;
import java.util.HashMap;

public class TestUtilities {
    public static DatasetVersion createDatasetVersion(String bagId, String nbn, int version, int versionMinor, String versionState) {
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

    public static DatasetVersion createDatasetVersionWithoutVaultMetadataBlock(int version, int versionMinor, String versionState) {
        var datasetVersion = new DatasetVersion();
        datasetVersion.setVersionNumber(version);
        datasetVersion.setVersionMinorNumber(versionMinor);
        datasetVersion.setVersionState(versionState);
        datasetVersion.setMetadataBlocks(new HashMap<>());

        var block = new MetadataBlock();
        block.setFields(new ArrayList<>());

        return datasetVersion;
    }
}
