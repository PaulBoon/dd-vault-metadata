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

import java.util.HashMap;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class VaultMetadataKey {
    private static final Logger log = LoggerFactory.getLogger(VaultMetadataKey.class);
    private static final String name = "dansDataVaultMetadata"; // the name of the metadata block
    private static final String paramNamePrefix = "mdkey.";
    private final String value;
    private final Boolean enabled; // indicate it is enabled (use the key) or not
    
    public VaultMetadataKey(String value, Boolean enabled) {
        this.value = value;
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public Boolean isEnabled() { return enabled; }

    public HashMap<String, List<String>> getQueryParams() {
        // construct HTTP request query parameter
        return new HashMap<String, List<String>>(singletonMap(paramNamePrefix + getName(), singletonList(getValue())));
    }
}
