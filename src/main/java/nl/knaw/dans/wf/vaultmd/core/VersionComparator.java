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

import java.util.Comparator;

public class VersionComparator implements Comparator<DatasetVersion> {
    @Override
    public int compare(DatasetVersion left, DatasetVersion right) {
        var leftParts = new int[] { left.getVersionNumber(), left.getVersionMinorNumber() };
        var rightParts = new int[] { right.getVersionNumber(), right.getVersionMinorNumber() };

        for (var i = 0; i < leftParts.length; ++i) {
            var result = Integer.compare(leftParts[i], rightParts[i]);

            if (result != 0) {
                return result;
            }
        }

        return 0;
    }
}
