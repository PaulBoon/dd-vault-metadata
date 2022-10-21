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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VersionComparatorTest {

    Pair<DatasetVersion, DatasetVersion> createVersions(String a, String b) {
        var d1 = new DatasetVersion();
        d1.setVersionNumber(Integer.parseInt(a.split("\\.")[0]));
        d1.setVersionMinorNumber(Integer.parseInt(a.split("\\.")[1]));

        var d2 = new DatasetVersion();
        d2.setVersionNumber(Integer.parseInt(b.split("\\.")[0]));
        d2.setVersionMinorNumber(Integer.parseInt(b.split("\\.")[1]));

        return Pair.of(d1, d2);
    }

    @Test
    void testLeftVersionLower() {
        var versions = createVersions("1.1", "1.2");
        assertEquals(-1, new VersionComparator().compare(versions.getLeft(), versions.getRight()));
    }

    @Test
    void testLeftVersionHigher() {
        var versions = createVersions("2.1", "1.2");
        assertEquals(1, new VersionComparator().compare(versions.getLeft(), versions.getRight()));
    }

    @Test
    void testVersionsEqual() {
        var versions = createVersions("7.18", "7.18");
        assertEquals(0, new VersionComparator().compare(versions.getLeft(), versions.getRight()));
    }
}