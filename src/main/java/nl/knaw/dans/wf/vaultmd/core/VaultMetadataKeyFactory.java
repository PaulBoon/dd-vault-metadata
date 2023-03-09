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

public class VaultMetadataKeyFactory {
    private static final Logger log = LoggerFactory.getLogger(VaultMetadataKeyFactory.class);
    private String name; 
    private String value;

    public VaultMetadataKeyFactory() {
    }
    
    public VaultMetadataKey build() {
        log.info("VaultMetadataKey (name, value): {}, {}", name, value);
        return new VaultMetadataKey(name, value);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
