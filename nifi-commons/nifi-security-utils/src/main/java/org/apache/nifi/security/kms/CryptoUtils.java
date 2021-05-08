/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.security.kms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.repository.config.RepositoryEncryptionConfiguration;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtils {
    private static final Logger logger = LoggerFactory.getLogger(CryptoUtils.class);
    public static final String STATIC_KEY_PROVIDER_CLASS_NAME = "org.apache.nifi.security.kms.StaticKeyProvider";
    public static final String FILE_BASED_KEY_PROVIDER_CLASS_NAME = "org.apache.nifi.security.kms.FileBasedKeyProvider";

    // TODO: Move to RepositoryEncryptionUtils in NIFI-6617
    public static final String LEGACY_SKP_FQCN = "org.apache.nifi.provenance.StaticKeyProvider";
    public static final String LEGACY_FBKP_FQCN = "org.apache.nifi.provenance.FileBasedKeyProvider";

    private static final String RELATIVE_NIFI_PROPS_PATH = "conf/nifi.properties";
    private static final String BOOTSTRAP_KEY_PREFIX = "nifi.bootstrap.sensitive.key=";

    // TODO: Enforce even length
    private static final Pattern HEX_PATTERN = Pattern.compile("(?i)^[0-9a-f]+$");

    private static final List<Integer> UNLIMITED_KEY_LENGTHS = Arrays.asList(32, 48, 64);

    public static final int IV_LENGTH = 16;
    public static final String ENCRYPTED_FSR_CLASS_NAME = "org.apache.nifi.controller.repository.crypto.EncryptedFileSystemRepository";
    public static final String EWAFFR_CLASS_NAME = "org.apache.nifi.controller.repository.crypto.EncryptedWriteAheadFlowFileRepository";

    public static boolean isUnlimitedStrengthCryptoAvailable() {
        try {
            return Cipher.getMaxAllowedKeyLength("AES") > 128;
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Tried to determine if unlimited strength crypto is available but the AES algorithm is not available");
            return false;
        }
    }

    /**
     * Utility method which returns true if the string is null, empty, or entirely whitespace.
     *
     * @param src the string to evaluate
     * @return true if empty
     */
    public static boolean isEmpty(String src) {
        return src == null || src.trim().isEmpty();
    }

    /**
     * Concatenates multiple byte[] into a single byte[].
     *
     * @param arrays the component byte[] in order
     * @return a concatenated byte[]
     * @throws IOException this should never be thrown
     */
    public static byte[] concatByteArrays(byte[]... arrays) throws IOException {
        int totalByteLength = 0;
        for (byte[] bytes : arrays) {
            totalByteLength += bytes.length;
        }
        byte[] totalBytes = new byte[totalByteLength];
        int currentLength = 0;
        for (byte[] bytes : arrays) {
            System.arraycopy(bytes, 0, totalBytes, currentLength, bytes.length);
            currentLength += bytes.length;
        }
        return totalBytes;
    }

    /**
     * Returns true if the provided configuration values are valid (shallow evaluation only; does not validate the keys
     * contained in a {@link FileBasedKeyProvider}).
     *
     * @param rec the configuration to validate
     * @return true if the config is valid
     */
    public static boolean isValidRepositoryEncryptionConfiguration(RepositoryEncryptionConfiguration rec) {
        return isValidKeyProvider(rec.getKeyProviderImplementation(), rec.getKeyProviderLocation(), rec.getEncryptionKeyId(), rec.getEncryptionKeys());

    }

    /**
     * Returns true if the provided configuration values successfully define the specified {@link KeyProvider}.
     *
     * @param keyProviderImplementation the FQ class name of the {@link KeyProvider} implementation
     * @param keyProviderLocation       the location of the definition (for {@link FileBasedKeyProvider}, etc.)
     * @param keyId                     the active key ID
     * @param encryptionKeys            a map of key IDs to key material in hex format
     * @return true if the provided configuration is valid
     */
    public static boolean isValidKeyProvider(String keyProviderImplementation, String keyProviderLocation, String keyId, Map<String, String> encryptionKeys) {
        logger.debug("Attempting to validate the key provider: keyProviderImplementation = "
                + keyProviderImplementation + ", keyProviderLocation = "
                + keyProviderLocation + ", keyId = "
                + keyId + ", encryptionKeys = "
                + ((encryptionKeys == null) ? "0" : encryptionKeys.size()));

        try {
            keyProviderImplementation = handleLegacyPackages(keyProviderImplementation);
        } catch (KeyManagementException e) {
            logger.warn("The attempt to validate the key provider failed keyProviderImplementation = "
                    + keyProviderImplementation + ", keyProviderLocation = "
                    + keyProviderLocation + ", keyId = "
                    + keyId + ", encryptionKeys = "
                    + ((encryptionKeys == null) ? "0" : encryptionKeys.size()));

            return false;
        }

        if (STATIC_KEY_PROVIDER_CLASS_NAME.equals(keyProviderImplementation)) {
            // Ensure the keyId and key(s) are valid
            if (encryptionKeys == null) {
                return false;
            } else {
                boolean everyKeyValid = encryptionKeys.values().stream().allMatch(CryptoUtils::keyIsValid);
                return everyKeyValid && StringUtils.isNotEmpty(keyId);
            }
        } else if (FILE_BASED_KEY_PROVIDER_CLASS_NAME.equals(keyProviderImplementation)) {
            // Ensure the file can be read and the keyId is populated (does not read file to validate)
            final File kpf = new File(keyProviderLocation);
            return kpf.exists() && kpf.canRead() && StringUtils.isNotEmpty(keyId);
        } else {
            logger.warn("The attempt to validate the key provider failed keyProviderImplementation = "
                    + keyProviderImplementation + ", keyProviderLocation = "
                    + keyProviderLocation + ", keyId = "
                    + keyId + ", encryptionKeys = "
                    + ((encryptionKeys == null) ? "0" : encryptionKeys.size()));

            return false;
        }
    }

    static String handleLegacyPackages(String implementationClassName) throws KeyManagementException {
        if (org.apache.nifi.util.StringUtils.isBlank(implementationClassName)) {
            throw new KeyManagementException("Invalid key provider implementation provided: " + implementationClassName);
        }
        if (implementationClassName.equalsIgnoreCase(LEGACY_SKP_FQCN)) {
            return StaticKeyProvider.class.getName();
        } else if (implementationClassName.equalsIgnoreCase(LEGACY_FBKP_FQCN)) {
            return FileBasedKeyProvider.class.getName();
        } else {
            return implementationClassName;
        }
    }

    /**
     * Returns true if the provided key is valid hex and is the correct length for the current system's JCE policies.
     *
     * @param encryptionKeyHex the key in hexadecimal
     * @return true if this key is valid
     */
    public static boolean keyIsValid(String encryptionKeyHex) {
        return isHexString(encryptionKeyHex)
                && (isUnlimitedStrengthCryptoAvailable()
                ? UNLIMITED_KEY_LENGTHS.contains(encryptionKeyHex.length())
                : encryptionKeyHex.length() == 32);
    }

    /**
     * Returns true if the input is valid hexadecimal (does not enforce length and is case-insensitive).
     *
     * @param hexString the string to evaluate
     * @return true if the string is valid hex
     */
    public static boolean isHexString(String hexString) {
        return StringUtils.isNotEmpty(hexString) && HEX_PATTERN.matcher(hexString).matches();
    }

    /**
     * Returns a {@link SecretKey} formed from the hexadecimal key bytes (validity is checked).
     *
     * @param keyHex the key in hex form
     * @return the SecretKey
     */
    public static SecretKey formKeyFromHex(String keyHex) throws KeyManagementException {
        if (keyIsValid(keyHex)) {
            return new SecretKeySpec(Hex.decode(keyHex), "AES");
        } else {
            throw new KeyManagementException("The provided key material is not valid");
        }
    }

    /**
     * Returns a map containing the key IDs and the parsed key from a key provider definition file.
     * The values in the file are decrypted using the root key provided. If the file is missing or empty,
     * cannot be read, or if no valid keys are read, a {@link KeyManagementException} will be thrown.
     *
     * @param filepath  the key definition file path
     * @param rootKey the root key used to decrypt each key definition
     * @return a Map of key IDs to SecretKeys
     * @throws KeyManagementException if the file is missing or invalid
     */
    public static Map<String, SecretKey> readKeys(String filepath, SecretKey rootKey) throws KeyManagementException {
        Map<String, SecretKey> keys = new HashMap<>();

        if (StringUtils.isBlank(filepath)) {
            throw new KeyManagementException("The key provider file is not present and readable");
        }
        if (rootKey == null) {
            throw new KeyManagementException("The root key must be provided to decrypt the individual keys");
        }

        File file = new File(filepath);
        if (!file.exists() || !file.canRead()) {
            throw new KeyManagementException("The key provider file is not present and readable");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            AESKeyedCipherProvider rootCipherProvider = new AESKeyedCipherProvider();

            String line;
            int l = 1;
            while ((line = br.readLine()) != null) {
                String[] components = line.split("=", 2);
                if (components.length != 2 || StringUtils.isAnyEmpty(components)) {
                    logger.warn("Line " + l + " is not properly formatted -- keyId=Base64EncodedKey...");
                }
                String keyId = components[0];
                if (StringUtils.isNotEmpty(keyId)) {
                    try {
                        byte[] base64Bytes = Base64.getDecoder().decode(components[1]);
                        byte[] ivBytes = Arrays.copyOfRange(base64Bytes, 0, IV_LENGTH);

                        Cipher rootCipher = null;
                        try {
                            rootCipher = rootCipherProvider.getCipher(EncryptionMethod.AES_GCM, rootKey, ivBytes, false);
                        } catch (Exception e) {
                            throw new KeyManagementException("Error building cipher to decrypt FileBaseKeyProvider definition at " + filepath, e);
                        }
                        byte[] individualKeyBytes = rootCipher.doFinal(Arrays.copyOfRange(base64Bytes, IV_LENGTH, base64Bytes.length));

                        SecretKey key = new SecretKeySpec(individualKeyBytes, "AES");
                        logger.debug("Read and decrypted key for " + keyId);
                        if (keys.containsKey(keyId)) {
                            logger.warn("Multiple key values defined for " + keyId + " -- using most recent value");
                        }
                        keys.put(keyId, key);
                    } catch (IllegalArgumentException e) {
                        logger.error("Encountered an error decoding Base64 for " + keyId + ": " + e.getLocalizedMessage());
                    } catch (BadPaddingException | IllegalBlockSizeException e) {
                        logger.error("Encountered an error decrypting key for " + keyId + ": " + e.getLocalizedMessage());
                    }
                }
                l++;
            }

            if (keys.isEmpty()) {
                throw new KeyManagementException("The provided file contained no valid keys");
            }

            logger.info("Read " + keys.size() + " keys from FileBasedKeyProvider " + filepath);
            return keys;
        } catch (IOException e) {
            throw new KeyManagementException("Error reading FileBasedKeyProvider definition at " + filepath, e);
        }

    }

    /**
     * Returns the root key from the {@code bootstrap.conf} file used to encrypt various sensitive properties and data encryption keys.
     *
     * @return the root key
     * @throws KeyManagementException if the key cannot be read
     */
    public static SecretKey getRootKey() throws KeyManagementException {
        try {
            // Get the root encryption key from bootstrap.conf
            String rootKeyHex = extractKeyFromBootstrapFile();
            return new SecretKeySpec(Hex.decode(rootKeyHex), "AES");
        } catch (IOException e) {
            logger.error("Encountered an error: ", e);
            throw new KeyManagementException(e);
        }
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public static String extractKeyFromBootstrapFile() throws IOException {
        return extractKeyFromBootstrapFile("");
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from {@code $NIFI_HOME/conf/bootstrap.conf}.
     *
     * @param bootstrapPath the path to the bootstrap file
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public static String extractKeyFromBootstrapFile(String bootstrapPath) throws IOException {
        File expectedBootstrapFile;
        if (StringUtils.isBlank(bootstrapPath)) {
            // Guess at location of bootstrap.conf file from nifi.properties file
            String defaultNiFiPropertiesPath = getDefaultFilePath();
            File propertiesFile = new File(defaultNiFiPropertiesPath);
            File confDir = new File(propertiesFile.getParent());
            if (confDir.exists() && confDir.canRead()) {
                expectedBootstrapFile = new File(confDir, "bootstrap.conf");
            } else {
                logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key -- conf/ directory is missing or permissions are incorrect", confDir.getAbsolutePath());
                throw new IOException("Cannot read from bootstrap.conf");
            }
        } else {
            expectedBootstrapFile = new File(bootstrapPath);
        }

        if (expectedBootstrapFile.exists() && expectedBootstrapFile.canRead()) {
            try (Stream<String> stream = Files.lines(Paths.get(expectedBootstrapFile.getAbsolutePath()))) {
                Optional<String> keyLine = stream.filter(l -> l.startsWith(BOOTSTRAP_KEY_PREFIX)).findFirst();
                if (keyLine.isPresent()) {
                    return keyLine.get().split("=", 2)[1];
                } else {
                    logger.warn("No encryption key present in the bootstrap.conf file at {}", expectedBootstrapFile.getAbsolutePath());
                    return "";
                }
            } catch (IOException e) {
                logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key", expectedBootstrapFile.getAbsolutePath());
                throw new IOException("Cannot read from bootstrap.conf", e);
            }
        } else {
            logger.error("Cannot read from bootstrap.conf file at {} to extract encryption key -- file is missing or permissions are incorrect", expectedBootstrapFile.getAbsolutePath());
            throw new IOException("Cannot read from bootstrap.conf");
        }
    }

    /**
     * Returns the default file path to {@code $NIFI_HOME/conf/nifi.properties}. If the system
     * property {@code nifi.properties.file.path} is not set, it will be set to the relative
     * path {@code conf/nifi.properties}.
     *
     * @return the path to the nifi.properties file
     */
    public static String getDefaultFilePath() {
        String systemPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH);

        if (systemPath == null || systemPath.trim().isEmpty()) {
            logger.warn("The system variable {} is not set, so it is being set to '{}'", NiFiProperties.PROPERTIES_FILE_PATH, RELATIVE_NIFI_PROPS_PATH);
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, RELATIVE_NIFI_PROPS_PATH);
            systemPath = RELATIVE_NIFI_PROPS_PATH;
        }

        logger.info("Determined default nifi.properties path to be '{}'", systemPath);
        return systemPath;
    }

    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     *
     * @param a a String to compare
     * @param b a String to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(String a, String b) {
        if (a == null) {
            return b == null;
        } else {
            // This returns true IFF b != null and the byte[] are equal; if b == null, a is not, and they are not equal
            return b != null && constantTimeEquals(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     * Does not convert the character arrays to {@code String}s when converting to {@code byte[]}
     * to avoid putting sensitive data in the String pool.
     *
     * @param a a char[] to compare
     * @param b a char[] to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(char[] a, char[] b) {
        return constantTimeEquals(convertCharsToBytes(a), convertCharsToBytes(b));
    }


    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     *
     * @param a a byte[] to compare
     * @param b a byte[] to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }

    /**
     * Returns a {@code byte[]} containing the value of the provided {@code char[]} without using {@code new String(chars).getBytes()} which would put sensitive data (the password) in the String pool.
     *
     * @param chars the characters to convert
     * @return the byte[]
     */
    private static byte[] convertCharsToBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
        return Arrays.copyOfRange(byteBuffer.array(),
                byteBuffer.position(), byteBuffer.limit());
    }
}
