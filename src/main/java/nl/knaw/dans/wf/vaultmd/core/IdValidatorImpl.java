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

import java.util.UUID;

public class IdValidatorImpl implements IdValidator {
    private static final String NBN_PREFIX = "urn:nbn:nl:ui:13-";
    private static final String UUID_PREFIX = "urn:uuid:";

    @Override
    public boolean isValidUrnNbn(String id) {
        if (id == null) {
            return false;
        }

        if (!id.toLowerCase().startsWith(NBN_PREFIX)) {
            return false;
        }

        try {
            // check if the UUID can be parsed
            UUID.fromString(id.substring(NBN_PREFIX.length()));
        }
        catch (IllegalArgumentException e) {
            return false;
        }

        return true;
    }

    @Override
    public boolean isValidUrnUuid(String id) {
        if (id == null) {
            return false;
        }

        if (!id.toLowerCase().startsWith(UUID_PREFIX)) {
            return false;
        }

        try {
            // check if the UUID can be parsed
            UUID.fromString(id.substring(UUID_PREFIX.length()));
        }
        catch (IllegalArgumentException e) {
            return false;
        }

        return true;
    }
}
