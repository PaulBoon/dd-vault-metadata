package nl.knaw.dans.wf.vaultmd.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdValidatorImplTest {

    @Test
    void isValidUrnUuid_should_return_true_for_valid_urn_uuids() {
        assertTrue(new IdValidatorImpl().isValidUrnUuid("urn:uuid:06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
        assertTrue(new IdValidatorImpl().isValidUrnUuid("URN:UUID:06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
    }

    @Test
    void isValidUrnUuid_should_return_false_for_all_other_strings() {
        assertFalse(new IdValidatorImpl().isValidUrnUuid(""));
        assertFalse(new IdValidatorImpl().isValidUrnUuid(null));
        assertFalse(new IdValidatorImpl().isValidUrnUuid("urn:nbn:06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
        assertFalse(new IdValidatorImpl().isValidUrnUuid("urn:uuid:nonuuid:06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
    }

    @Test
    void isValidUrnNbn_should_return_true_for_valid_urn_uuids() {
        assertTrue(new IdValidatorImpl().isValidUrnNbn("urn:nbn:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
        assertTrue(new IdValidatorImpl().isValidUrnNbn("URN:NBN:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
    }

    @Test
    void isValidUrnNbn_should_return_false_for_invalid_urn_uuids() {
        assertFalse(new IdValidatorImpl().isValidUrnNbn(null));
        assertFalse(new IdValidatorImpl().isValidUrnNbn(""));
        assertFalse(new IdValidatorImpl().isValidUrnNbn("   URN:NBN:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
        assertFalse(new IdValidatorImpl().isValidUrnNbn("URN:NBN:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e    "));
        assertFalse(new IdValidatorImpl().isValidUrnNbn("urn:uuid:nl:ui:13-"));
        assertFalse(new IdValidatorImpl().isValidUrnNbn("URN:XXX:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e"));
        assertFalse(new IdValidatorImpl().isValidUrnNbn("urn:nbn:nl:ui:13-06aeda5a-c0b1-4612-ae9a-43d682c51e1e-asdf"));
    }
}