package nl.knaw.dans.wf.vaultmd.core;

public interface IdValidator {

    boolean isValidUrnNbn(String id);

    boolean isValidUrnUuid(String id);
}
