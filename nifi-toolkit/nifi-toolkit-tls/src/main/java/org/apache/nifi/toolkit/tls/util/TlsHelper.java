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

package org.apache.nifi.toolkit.tls.util;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.DERNull;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.pkcs.RSAPrivateKey;
import org.bouncycastle.asn1.pkcs.RSAPublicKey;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x500.style.IETFUtils;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.ExtensionsGenerator;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.IPAddress;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

public class TlsHelper {
    private static final Logger logger = LoggerFactory.getLogger(TlsHelper.class);
    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;
    public static final String JCE_URL = "http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html";
    public static final String ILLEGAL_KEY_SIZE = "illegal key size";
    private static boolean isUnlimitedStrengthCryptographyEnabled;
    private static boolean isVerbose = true;

    // Evaluate an unlimited strength algorithm to determine if we support the capability we have on the system
    static {
        try {
            isUnlimitedStrengthCryptographyEnabled = (Cipher.getMaxAllowedKeyLength("AES") > DEFAULT_MAX_ALLOWED_KEY_LENGTH);
        } catch (NoSuchAlgorithmException e) {
            // if there are issues with this, we default back to the value established
            isUnlimitedStrengthCryptographyEnabled = false;
        }
    }

    private static void logTruncationWarning(File file) {
        String fileToString = file.toString();
        String fileName = file.getName();
        logger.warn("**********************************************************************************");
        logger.warn("                                    WARNING!!!!");
        logger.warn("**********************************************************************************");
        logger.warn("Unlimited JCE Policy is not installed which means we cannot utilize a");
        logger.warn("PKCS12 password longer than 7 characters.");
        logger.warn("Autogenerated password has been reduced to 7 characters.");
        logger.warn("");
        logger.warn("Please strongly consider installing Unlimited JCE Policy at");
        logger.warn(JCE_URL);
        logger.warn("");
        logger.warn("Another alternative is to add a stronger password with the openssl tool to the");
        logger.warn("resulting client certificate: " + fileToString);
        logger.warn("");
        logger.warn("openssl pkcs12 -in '" + fileToString + "' -out '/tmp/" + fileName + "'");
        logger.warn("openssl pkcs12 -export -in '/tmp/" + fileName + "' -out '" + fileToString + "'");
        logger.warn("rm -f '/tmp/" + fileName + "'");
        logger.warn("");
        logger.warn("**********************************************************************************");

    }

    private TlsHelper() {

    }

    public static boolean isUnlimitedStrengthCryptographyEnabled() {
        return isUnlimitedStrengthCryptographyEnabled;
    }

    public static String writeKeyStore(KeyStore keyStore, OutputStreamFactory outputStreamFactory, File file, String password, boolean generatedPassword) throws IOException, GeneralSecurityException {
        try (OutputStream fileOutputStream = outputStreamFactory.create(file)) {
            keyStore.store(fileOutputStream, password.toCharArray());
        } catch (IOException e) {
            if (e.getMessage().toLowerCase().contains(ILLEGAL_KEY_SIZE) && !isUnlimitedStrengthCryptographyEnabled()) {
                if (generatedPassword) {
                    file.delete();
                    String truncatedPassword = password.substring(0, 7);
                    try (OutputStream fileOutputStream = outputStreamFactory.create(file)) {
                        keyStore.store(fileOutputStream, truncatedPassword.toCharArray());
                    }
                    logTruncationWarning(file);
                    return truncatedPassword;
                } else {
                    throw new GeneralSecurityException("Specified password for " + file + " too long to work without unlimited JCE policy installed."
                            + System.lineSeparator() + "Please see " + JCE_URL);
                }
            } else {
                throw e;
            }
        }
        return password;
    }

    public static HashMap<String, Certificate> extractCerts(KeyStore keyStore) throws KeyStoreException {
        HashMap<String, Certificate> certs = new HashMap<>();
        Enumeration<String> certAliases = keyStore.aliases();
        while(certAliases.hasMoreElements()) {
            String alias = certAliases.nextElement();
            certs.put(alias, keyStore.getCertificate(alias));
        }
        return certs;
    }

    public static HashMap<String, Key> extractKeys(KeyStore keyStore, char[] privKeyPass) throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        HashMap<String, Key> keys = new HashMap<>();
        Enumeration<String> keyAliases = keyStore.aliases();
        while(keyAliases.hasMoreElements()) {
            String alias = keyAliases.nextElement();
            Key key = keyStore.getKey(alias, privKeyPass);
            if(key != null) {
                keys.put(alias, key);
            } else {
                logger.warn("Key does not exist: Certificate with alias '" + alias + "' had no private key.");
            }
        }
        return keys;
    }

    public static void outputCertsAsPem(HashMap<String, Certificate> certs, File directory, String extension) {
        certs.forEach((String alias, Certificate cert)->{
            try {
                TlsHelper.outputAsPem(cert, alias, directory, extension);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static void outputKeysAsPem(HashMap<String, Key> keys, File directory, String extension) {
        keys.forEach((String alias, Key key) -> {
            try {
                TlsHelper.outputAsPem(key, alias, directory, extension);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void outputAsPem(Object pemObj, String filename, File directory, String extension) throws IOException {
        OutputStream outputStream = new FileOutputStream(new File(directory,  TlsHelper.escapeFilename(filename) + extension));
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
        JcaPEMWriter pemWriter = new JcaPEMWriter(outputStreamWriter);
        JcaMiscPEMGenerator pemGen = new JcaMiscPEMGenerator(pemObj);
        pemWriter.writeObject(pemGen);
        pemWriter.close();
    }

    private static KeyPairGenerator createKeyPairGenerator(String algorithm, int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator instance = KeyPairGenerator.getInstance(algorithm);
        instance.initialize(keySize);
        return instance;
    }

    public static byte[] calculateHMac(String token, PublicKey publicKey) throws GeneralSecurityException {
        if (token == null) {
            throw new IllegalArgumentException("Token cannot be null");
        }
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
        if (tokenBytes.length < 16) {
            throw new GeneralSecurityException("Token does not meet minimum size of 16 bytes.");
        }
        SecretKeySpec keySpec = new SecretKeySpec(tokenBytes, "RAW");
        Mac mac = Mac.getInstance("Hmac-SHA256", BouncyCastleProvider.PROVIDER_NAME);
        mac.init(keySpec);
        return mac.doFinal(getKeyIdentifier(publicKey));
    }

    public static byte[] getKeyIdentifier(PublicKey publicKey) throws NoSuchAlgorithmException {
        return new JcaX509ExtensionUtils().createSubjectKeyIdentifier(publicKey).getKeyIdentifier();
    }

    public static String pemEncodeJcaObject(Object object) throws IOException {
        StringWriter writer = new StringWriter();
        try (PemWriter pemWriter = new PemWriter(writer)) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(object));
        }
        return writer.toString();
    }

    public static JcaPKCS10CertificationRequest parseCsr(String pemEncodedCsr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(pemEncodedCsr))) {
            Object o = pemParser.readObject();
            if (!PKCS10CertificationRequest.class.isInstance(o)) {
                throw new IOException("Expecting instance of " + PKCS10CertificationRequest.class + " but got " + o);
            }
            return new JcaPKCS10CertificationRequest((PKCS10CertificationRequest) o);
        }
    }

    public static X509Certificate parseCertificate(Reader pemEncodedCertificate) throws IOException, CertificateException {
        return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(parsePem(X509CertificateHolder.class, pemEncodedCertificate));
    }

    /**
     * Returns the parsed {@link KeyPair} from the provided {@link Reader}. The incoming format can be PKCS #8 or PKCS #1.
     *
     * @param pemKeyPairReader a reader with access to the serialized key pair
     * @return the key pair
     * @throws IOException if there is an error reading the key pair
     */
    public static KeyPair parseKeyPairFromReader(Reader pemKeyPairReader) throws IOException {
        // Instantiate PEMParser from Reader
        try (PEMParser pemParser = new PEMParser(pemKeyPairReader)) {
            // Read the object (deserialize)
            Object parsedObject = pemParser.readObject();

            // If this is an ASN.1 private key, it's in PKCS #8 format and wraps the actual RSA private key
            if (PrivateKeyInfo.class.isInstance(parsedObject)) {
                if (isVerbose()) {
                    logger.info("Provided private key is in PKCS #8 format");
                }
                PEMKeyPair keyPair = convertPrivateKeyFromPKCS8ToPKCS1((PrivateKeyInfo) parsedObject);
                return getKeyPair(keyPair);
            } else if (PEMKeyPair.class.isInstance(parsedObject)) {
                // Already in PKCS #1 format
                return getKeyPair((PEMKeyPair)parsedObject);
            } else {
                logger.warn("Expected one of %s or %s but got %s", PrivateKeyInfo.class, PEMKeyPair.class, parsedObject.getClass());
                throw new IOException("Expected private key in PKCS #1 or PKCS #8 unencrypted format");
            }
        }
    }

    /**
     * Returns a {@link KeyPair} instance containing the {@link X509Certificate} public key and the {@link java.security.spec.PKCS8EncodedKeySpec} private key from the PEM-encoded {@link PEMKeyPair}.
     *
     * @param keyPair the key pair in PEM format
     * @return the key pair in a format which provides for direct access to the keys
     * @throws PEMException if there is an error converting the key pair
     */
    private static KeyPair getKeyPair(PEMKeyPair keyPair) throws PEMException {
        return new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getKeyPair(keyPair);
    }

    /**
     * Returns a {@link PEMKeyPair} object with direct access to the public and private keys given a PKCS #8 private key.
     *
     * @param privateKeyInfo the PKCS #8 private key info
     * @return the PKCS #1 public and private key pair
     * @throws IOException if there is an error converting the key pair
     */
    private static PEMKeyPair convertPrivateKeyFromPKCS8ToPKCS1(PrivateKeyInfo privateKeyInfo) throws IOException {
        // Parse the key wrapping to determine the internal key structure
        ASN1Encodable asn1PrivateKey = privateKeyInfo.parsePrivateKey();

        // Convert the parsed key to an RSA private key
        RSAPrivateKey keyStruct = RSAPrivateKey.getInstance(asn1PrivateKey);

        // Create the RSA public key from the modulus and exponent
        RSAPublicKey pubSpec = new RSAPublicKey(
            keyStruct.getModulus(), keyStruct.getPublicExponent());

        // Create an algorithm identifier for forming the key pair
        AlgorithmIdentifier algId = new AlgorithmIdentifier(PKCSObjectIdentifiers.rsaEncryption, DERNull.INSTANCE);
        if (isVerbose()) {
            logger.info("Converted private key from PKCS #8 to PKCS #1 RSA private key");
        }

        // Create the key pair container
        return new PEMKeyPair(new SubjectPublicKeyInfo(algId, pubSpec), new PrivateKeyInfo(algId, keyStruct));
    }

    public static <T> T parsePem(Class<T> clazz, Reader pemReader) throws IOException {
        try (PEMParser pemParser = new PEMParser(pemReader)) {
            Object object = pemParser.readObject();
            if (!clazz.isInstance(object)) {
                throw new IOException("Expected " + clazz + " but got " + object.getClass());
            }
            return (T) object;
        }
    }

    public static KeyPair generateKeyPair(String algorithm, int keySize) throws NoSuchAlgorithmException {
        return createKeyPairGenerator(algorithm, keySize).generateKeyPair();
    }

    public static JcaPKCS10CertificationRequest generateCertificationRequest(String requestedDn, List<String> domainAlternativeNames,
                                                                             KeyPair keyPair, String signingAlgorithm) throws OperatorCreationException {
        JcaPKCS10CertificationRequestBuilder jcaPKCS10CertificationRequestBuilder = new JcaPKCS10CertificationRequestBuilder(new X500Name(requestedDn), keyPair.getPublic());

        // add Subject Alternative Name(s)
        try {
            jcaPKCS10CertificationRequestBuilder.addAttribute(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest, createDomainAlternativeNamesExtensions(domainAlternativeNames, requestedDn));
        } catch (IOException e) {
            throw new OperatorCreationException("Error while adding " + domainAlternativeNames + " as Subject Alternative Name.", e);
        }

        JcaContentSignerBuilder jcaContentSignerBuilder = new JcaContentSignerBuilder(signingAlgorithm);
        return new JcaPKCS10CertificationRequest(jcaPKCS10CertificationRequestBuilder.build(jcaContentSignerBuilder.build(keyPair.getPrivate())));
    }

    public static Extensions createDomainAlternativeNamesExtensions(List<String> domainAlternativeNames, String requestedDn) throws IOException {
        List<GeneralName> namesList = new ArrayList<>();

        try {
            final String cn = IETFUtils.valueToString(new X500Name(requestedDn).getRDNs(BCStyle.CN)[0].getFirst().getValue());
            namesList.add(new GeneralName(GeneralName.dNSName, cn));
        } catch (Exception e) {
            throw new IOException("Failed to extract CN from request DN: " + requestedDn, e);
        }

        if (domainAlternativeNames != null) {
            for (String alternativeName : domainAlternativeNames) {
                 namesList.add(new GeneralName(IPAddress.isValid(alternativeName) ? GeneralName.iPAddress : GeneralName.dNSName, alternativeName));
             }
        }

        GeneralNames subjectAltNames = new GeneralNames(namesList.toArray(new GeneralName[]{}));
        ExtensionsGenerator extGen = new ExtensionsGenerator();
        extGen.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
        return extGen.generate();
    }


    /**
     * Removes special characters (particularly forward and back slashes) from strings that become file names.
     *
     * @param filename A filename you plan to write to disk which needs to be escaped.
     * @return String with special characters converted to underscores.
     */
    public static final String escapeFilename(String filename) {
        return filename.replaceAll("[^\\w\\.\\-\\=]+", "_");
    }

    /**
     * Returns true if the {@code certificate} is signed by one of the {@code signingCertificates}. The list should
     * include the certificate itself to allow for self-signed certificates. If it does not, a self-signed certificate
     * will return {@code false}.
     *
     * @param certificate the certificate containing the signature being verified
     * @param signingCertificates a list of certificates which may have signed the certificate
     * @return true if one of the signing certificates did sign the certificate
     */
    public static boolean verifyCertificateSignature(X509Certificate certificate, List<X509Certificate> signingCertificates) {
        String certificateDisplayInfo = getCertificateDisplayInfo(certificate);
        if (isVerbose()) {
            logger.info("Verifying the certificate signature for " + certificateDisplayInfo);
        }
        boolean signatureMatches = false;
        for (X509Certificate signingCert : signingCertificates) {
            final String signingCertDisplayInfo = getCertificateDisplayInfo(signingCert);
            try {
                if (isVerbose()) {
                    logger.info("Attempting to verify certificate " + certificateDisplayInfo + " signature with " + signingCertDisplayInfo);
                }
                PublicKey pub = signingCert.getPublicKey();
                certificate.verify(pub);
                if (isVerbose()) {
                    logger.info("Certificate was signed by " + signingCertDisplayInfo);
                }
                signatureMatches = true;
                break;
            } catch (Exception e) {
                // Expected if the signature does not match
                if (isVerbose()) {
                    logger.warn("Certificate " + certificateDisplayInfo + " not signed by " + signingCertDisplayInfo + " [" + e.getLocalizedMessage() + "]");
                }
            }
        }
        return signatureMatches;
    }

    private static String getCertificateDisplayInfo(X509Certificate certificate) {
        return certificate.getSubjectX500Principal().getName();
    }

    private static boolean isVerbose() {
        // TODO: When verbose mode is enabled via command-line flag, this will read the variable
        return isVerbose;
    }

}
