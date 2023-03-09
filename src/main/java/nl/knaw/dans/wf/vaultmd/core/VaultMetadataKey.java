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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class VaultMetadataKey {
    private static final Logger log = LoggerFactory.getLogger(VaultMetadataKey.class);
    private static final String name = "dansDataVaultMetadata";
    private static final String paramNamePrefix = "mdkey.";
    private final String value;
    
    public VaultMetadataKey(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
    
    // construct HTTP request query parameter
    public Map<String, List<String>> getQueryParams() {
        return singletonMap(paramNamePrefix + getName(), singletonList(getValue()));
    }
}
