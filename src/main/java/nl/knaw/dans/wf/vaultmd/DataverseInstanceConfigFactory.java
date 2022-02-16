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
package nl.knaw.dans.wf.vaultmd;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.dans.lib.dataverse.DataverseClientConfig;

import java.net.URI;

public class DataverseInstanceConfigFactory {

    private URI baseUrl;
    private int apiVersion;
    private int connectionTimeoutMs;
    private int readTimeoutMs;
    private String apiKey;
    private int awaitLockStateMaxNumberOfRetries;
    private int awaitLockStateMillisecondsBetweenRetries;

    public DataverseInstanceConfigFactory() {
    }

    public DataverseClientConfig build() {
        return new DataverseClientConfig(baseUrl, apiKey, connectionTimeoutMs, readTimeoutMs);
    }

    @JsonProperty
    public void setBaseUrl(URI baseUrl) {
        this.baseUrl = baseUrl;
    }

    @JsonProperty
    public void setApiVersion(int apiVersion) {
        this.apiVersion = apiVersion;
    }

    @JsonProperty
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    @JsonProperty
    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    @JsonProperty
    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    @JsonProperty
    public void setAwaitLockStateMaxNumberOfRetries(int awaitLockStateMaxNumberOfRetries) {
        this.awaitLockStateMaxNumberOfRetries = awaitLockStateMaxNumberOfRetries;
    }

    @JsonProperty
    public void setAwaitLockStateMillisecondsBetweenRetries(int awaitLockStateMillisecondsBetweenRetries) {
        this.awaitLockStateMillisecondsBetweenRetries = awaitLockStateMillisecondsBetweenRetries;
    }
}
