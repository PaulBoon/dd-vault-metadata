package nl.knaw.dans.wf.vaultmd.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ResourceConfigurationSourceProvider;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import nl.knaw.dans.wf.vaultmd.DdVaultMetadataConfiguration;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DdVaultMetadataConfigurationTest {
    private final YamlConfigurationFactory<DdVaultMetadataConfiguration> factory;

    {
        ObjectMapper mapper = Jackson.newObjectMapper().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        factory = new YamlConfigurationFactory<>(DdVaultMetadataConfiguration.class, Validators.newValidator(), mapper, "dw");
    }

    @Test
    public void canReadAssembly() throws IOException, ConfigurationException {
        factory.build(FileInputStream::new, "src/main/assembly/dist/cfg/config.yml");
    }

//    @Test
//    public void canReadTest() throws IOException, ConfigurationException {
//        factory.build(new ResourceConfigurationSourceProvider(), "debug-etc/config.yml");
//    }

    @Test
    public void canReadVaultMetadataKey() throws IOException, ConfigurationException {
        var config = factory.build(new ResourceConfigurationSourceProvider(), "unit-test-config.yml");
        //assertEquals("http://localhost:8080/", config.getDataverse().getBaseUrl().toString());
        var mdKey = config.getVaultMetadataKey().build();
        assertEquals("somevalue", mdKey.getValue());
    }
}
