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
