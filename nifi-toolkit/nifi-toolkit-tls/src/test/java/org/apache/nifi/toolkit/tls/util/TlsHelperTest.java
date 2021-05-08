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

import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.bouncycastle.asn1.pkcs.Attribute;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.Extensions;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;
import org.bouncycastle.util.IPAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.KeyStoreSpi;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TlsHelperTest {
    public static final Logger logger = LoggerFactory.getLogger(TlsHelperTest.class);

    private static final boolean originalUnlimitedCrypto = TlsHelper.isUnlimitedStrengthCryptographyEnabled();

    private int days;

    private int keySize;

    private String keyPairAlgorithm;

    private String signingAlgorithm;

    private KeyPairGenerator keyPairGenerator;

    private KeyStore keyStore;

    private String password = "changeit";

    @Mock
    KeyStoreSpi keyStoreSpi;

    @Mock
    Provider keyStoreProvider;

    @Mock
    OutputStreamFactory outputStreamFactory;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ByteArrayOutputStream tmpFileOutputStream;

    private File file;

    private static void setUnlimitedCrypto(boolean value) {
        try {
            Field isUnlimitedStrengthCryptographyEnabled = TlsHelper.class.getDeclaredField("isUnlimitedStrengthCryptographyEnabled");
            isUnlimitedStrengthCryptographyEnabled.setAccessible(true);
            isUnlimitedStrengthCryptographyEnabled.set(null, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static KeyPair loadKeyPair(Reader reader) throws IOException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(PEMKeyPair.class, object.getClass());
            return new JcaPEMKeyConverter().getKeyPair((PEMKeyPair) object);
        }
    }

    public static KeyPair loadKeyPair(File file) throws IOException {
        return loadKeyPair(new FileReader(file));
    }

    public static X509Certificate loadCertificate(Reader reader) throws IOException, CertificateException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object object = pemParser.readObject();
            assertEquals(X509CertificateHolder.class, object.getClass());
            return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate((X509CertificateHolder) object);
        }
    }

    public static X509Certificate loadCertificate(File file) throws IOException, CertificateException {
        return loadCertificate(new FileReader(file));
    }

    @Before
    public void setup() throws Exception {
        days = 360;
        keySize = 2048;
        keyPairAlgorithm = "RSA";
        signingAlgorithm = "SHA1WITHRSA";
        keyPairGenerator = KeyPairGenerator.getInstance(keyPairAlgorithm);
        keyPairGenerator.initialize(keySize);
        Constructor<KeyStore> keyStoreConstructor = KeyStore.class.getDeclaredConstructor(KeyStoreSpi.class, Provider.class, String.class);
        keyStoreConstructor.setAccessible(true);
        keyStore = keyStoreConstructor.newInstance(keyStoreSpi, keyStoreProvider, "faketype");
        keyStore.load(null, null);
        file = File.createTempFile("keystore", "file");
        when(outputStreamFactory.create(file)).thenReturn(tmpFileOutputStream);
    }

    @After
    public void tearDown() {
        setUnlimitedCrypto(originalUnlimitedCrypto);
        file.delete();
    }

    private Date inFuture(int days) {
        return new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(days));
    }

    @Test
    public void testTokenLengthInCalculateHmac() throws CertificateException, NoSuchAlgorithmException {
        List<String> badTokens = new ArrayList<>();
        List<String> goodTokens = new ArrayList<>();
        badTokens.add(null);
        badTokens.add("");
        badTokens.add("123");
        goodTokens.add("0123456789abcdefghijklm");
        goodTokens.add("0123456789abcdef");

        String dn = "CN=testDN,O=testOrg";
        X509Certificate x509Certificate = CertificateUtils.generateSelfSignedX509Certificate(TlsHelper.generateKeyPair(keyPairAlgorithm, keySize), dn, signingAlgorithm, days);
        PublicKey pubKey = x509Certificate.getPublicKey();

        for (String token : badTokens) {
            try {
                TlsHelper.calculateHMac(token, pubKey);
                fail("HMAC was calculated with a token that was too short.");
            } catch (GeneralSecurityException e) {
                assertEquals("Token does not meet minimum size of 16 bytes.", e.getMessage());
            } catch (IllegalArgumentException e) {
                assertEquals("Token cannot be null", e.getMessage());
            }
        }

        for (String token : goodTokens) {
            try {
                byte[] hmac = TlsHelper.calculateHMac(token, pubKey);
                assertTrue("HMAC length ok", hmac.length > 0);
            } catch (GeneralSecurityException e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testGenerateSelfSignedCert() throws GeneralSecurityException, IOException, OperatorCreationException {
        String dn = "CN=testDN,O=testOrg";

        X509Certificate x509Certificate = CertificateUtils.generateSelfSignedX509Certificate(TlsHelper.generateKeyPair(keyPairAlgorithm, keySize), dn, signingAlgorithm, days);

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(dn, x509Certificate.getIssuerX500Principal().getName());
        assertEquals(signingAlgorithm, x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.checkValidity();
    }

    @Test
    public void testIssueCert() throws IOException, CertificateException, NoSuchAlgorithmException, OperatorCreationException, NoSuchProviderException, InvalidKeyException, SignatureException {
        X509Certificate issuer = loadCertificate(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.crt")));
        KeyPair issuerKeyPair = loadKeyPair(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("rootCert.key")));

        String dn = "CN=testIssued, O=testOrg";

        KeyPair keyPair = TlsHelper.generateKeyPair(keyPairAlgorithm, keySize);
        X509Certificate x509Certificate = CertificateUtils.generateIssuedCertificate(dn, keyPair.getPublic(), issuer, issuerKeyPair, signingAlgorithm, days);
        assertEquals(dn, x509Certificate.getSubjectX500Principal().toString());
        assertEquals(issuer.getSubjectX500Principal().toString(), x509Certificate.getIssuerX500Principal().toString());
        assertEquals(keyPair.getPublic(), x509Certificate.getPublicKey());

        Date notAfter = x509Certificate.getNotAfter();
        assertTrue(notAfter.after(inFuture(days - 1)));
        assertTrue(notAfter.before(inFuture(days + 1)));

        Date notBefore = x509Certificate.getNotBefore();
        assertTrue(notBefore.after(inFuture(-1)));
        assertTrue(notBefore.before(inFuture(1)));

        assertEquals(signingAlgorithm, x509Certificate.getSigAlgName());
        assertEquals(keyPairAlgorithm, x509Certificate.getPublicKey().getAlgorithm());

        x509Certificate.verify(issuerKeyPair.getPublic());
    }

    @Test
    public void testWriteKeyStoreSuccess() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        assertEquals(testPassword, TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, false));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
    }

    @Test
    public void testWriteKeyStoreFailure() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        IOException ioException = new IOException("Fail");
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException);
        } catch (IOException e) {
            assertEquals(ioException, e);
        }
    }

    @Test
    public void testWriteKeyStoreTruncate() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        String truncatedPassword = testPassword.substring(0, 7);
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        assertEquals(truncatedPassword, TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        verify(keyStoreSpi, times(1)).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(truncatedPassword.toCharArray()));
    }

    @Test
    public void testWriteKeyStoreUnlimitedWontTruncate() throws GeneralSecurityException, IOException {
        setUnlimitedCrypto(true);
        String testPassword = "testPassword";
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException);
        } catch (IOException e) {
            assertEquals(ioException, e);
        }
    }

    @Test
    public void testWriteKeyStoreNoTruncate() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, false);
            fail("Expected " + GeneralSecurityException.class);
        } catch (GeneralSecurityException e) {
            assertTrue("Expected exception to contain " + TlsHelper.JCE_URL, e.getMessage().contains(TlsHelper.JCE_URL));
        }
    }

    @Test
    public void testWriteKeyStoreTruncateFailure() throws IOException, GeneralSecurityException {
        setUnlimitedCrypto(false);
        String testPassword = "testPassword";
        String truncatedPassword = testPassword.substring(0, 7);
        IOException ioException = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        IOException ioException2 = new IOException(TlsHelper.ILLEGAL_KEY_SIZE);
        doThrow(ioException).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(testPassword.toCharArray()));
        doThrow(ioException2).when(keyStoreSpi).engineStore(eq(tmpFileOutputStream), AdditionalMatchers.aryEq(truncatedPassword.toCharArray()));
        try {
            TlsHelper.writeKeyStore(keyStore, outputStreamFactory, file, testPassword, true);
            fail("Expected " + ioException2);
        } catch (IOException e) {
            assertEquals(ioException2, e);
        }
    }

    @Test
    public void testShouldIncludeSANFromCSR() throws Exception {
        // Arrange
        final List<String> SAN_ENTRIES = Arrays.asList("127.0.0.1", "nifi.nifi.apache.org");
        final int SAN_COUNT = SAN_ENTRIES.size();
        final String DN = "CN=localhost";
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        logger.info("Generating CSR with DN: " + DN);

        // Act
        JcaPKCS10CertificationRequest csrWithSan = TlsHelper.generateCertificationRequest(DN, SAN_ENTRIES, keyPair, TlsConfig.DEFAULT_SIGNING_ALGORITHM);
        logger.info("Created CSR with SAN: " + SAN_ENTRIES);
        String testCsrPem = TlsHelper.pemEncodeJcaObject(csrWithSan);
        logger.info("Encoded CSR as PEM: " + testCsrPem);

        // Assert
        String subjectName = csrWithSan.getSubject().toString();
        logger.info("CSR Subject Name: " + subjectName);
        assert subjectName.equals(DN);

        List<String> extractedSans = extractSanFromCsr(csrWithSan);
        assert extractedSans.size() == SAN_COUNT + 1;
        List<String> formattedSans = SAN_ENTRIES.stream()
                .map(s -> (IPAddress.isValid(s) ? "IP Address: " + new GeneralName(GeneralName.iPAddress, s).getName() : "DNS: " + s))
                .collect(Collectors.toList());
        assert extractedSans.containsAll(formattedSans);

        // We check that the SANs also contain the CN
        assert extractedSans.contains("DNS: localhost");
    }

    private List<String> extractSanFromCsr(JcaPKCS10CertificationRequest csr) {
        List<String> sans = new ArrayList<>();
        Attribute[] certAttributes = csr.getAttributes();
        for (Attribute attribute : certAttributes) {
            if (attribute.getAttrType().equals(PKCSObjectIdentifiers.pkcs_9_at_extensionRequest)) {
                Extensions extensions = Extensions.getInstance(attribute.getAttrValues().getObjectAt(0));
                GeneralNames gns = GeneralNames.fromExtensions(extensions, Extension.subjectAlternativeName);
                GeneralName[] names = gns.getNames();
                for (GeneralName name : names) {
                    logger.info("Type: " + name.getTagNo() + " | Name: " + name.getName());
                    String title = "";
                    if (name.getTagNo() == GeneralName.dNSName) {
                        title = "DNS";
                    } else if (name.getTagNo() == GeneralName.iPAddress) {
                        title = "IP Address";
                        // name.toASN1Primitive();
                    } else if (name.getTagNo() == GeneralName.otherName) {
                        title = "Other Name";
                    }
                    sans.add(title + ": " + name.getName());
                }
            }
        }

        return sans;
    }

    @Test
    public void testEscapeAliasFilenameWithForwardSlashes() {
        String result = TlsHelper.escapeFilename("my/silly/filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithBackSlashes() {
        String result = TlsHelper.escapeFilename("my\\silly\\filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameWithDollarSign() {
        String result = TlsHelper.escapeFilename("my$illyfilename.pem");
        assertEquals("my_illyfilename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameTwoSymbolsInARow() {
        String result = TlsHelper.escapeFilename("my!?sillyfilename.pem");
        assertEquals("my_sillyfilename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameKeepHyphens() {
        String result = TlsHelper.escapeFilename("my-silly-filename.pem");
        assertEquals("my-silly-filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameDoubleSpaces() {
        String result = TlsHelper.escapeFilename("my  silly  filename.pem");
        assertEquals("my_silly_filename.pem", result);
    }

    @Test
    public void testEscapeAliasFilenameSymbols() {
        String result = TlsHelper.escapeFilename("./\\!@#$%^&*()_-+=.pem");
        assertEquals(".__-_=.pem", result);
    }

    @Test
    public void testClientDnFilenameSlashes() throws Exception {
        String clientDn = "OU=NiFi/Organisation,CN=testuser";
        String escapedClientDn = TlsHelper.escapeFilename(CertificateUtils.reorderDn(clientDn));

        assertEquals("CN=testuser_OU=NiFi_Organisation", escapedClientDn);
    }

    @Test
    public void testClientDnFilenameSpecialChars() throws Exception {
        String clientDn = "OU=NiFi#!Organisation,CN=testuser";
        String escapedClientDn = TlsHelper.escapeFilename(CertificateUtils.reorderDn(clientDn));

        assertEquals("CN=testuser_OU=NiFi_Organisation", escapedClientDn);
    }

    private KeyStore setupKeystore() throws CertificateException, NoSuchAlgorithmException, IOException, KeyStoreException {

        KeyStore ks = KeyStore.getInstance("JKS");
        InputStream readStream = getClass().getClassLoader().getResourceAsStream("keystore.jks");
        ks.load(readStream, password.toCharArray());

        return ks;
    }

    @Test
    public void testOutputToFileTwoCertsAsPem() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        File folder = tempFolder.newFolder("splitKeystoreOutputDir");

        KeyStore keyStore = setupKeystore();
        HashMap<String, Certificate> certs = TlsHelper.extractCerts(keyStore);
        TlsHelper.outputCertsAsPem(certs, folder,".crt");

        assertEquals(folder.listFiles().length, 2);

        for(File file : folder.listFiles()) {
            X509Certificate certFromFile = loadCertificate(file);
            assertTrue(certs.containsValue(certFromFile));
            X509Certificate originalCert = (X509Certificate) certs.get(file.getName().split("\\.")[0]);
            assertTrue(originalCert.equals(certFromFile));
            assertArrayEquals(originalCert.getSignature(), certFromFile.getSignature());
        }
    }

    // Keystore contains two certificates, but one key. This is to test the edge case where a certificate does not have a key.
    @Test
    public void testOutputToFileOneKeyAsPem() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
        File folder = tempFolder.newFolder("splitKeystoreOutputDir");
        KeyStore keyStore = setupKeystore();
        HashMap<String, Key> keys = TlsHelper.extractKeys(keyStore, password.toCharArray());
        TlsHelper.outputKeysAsPem(keys, folder, ".key");

        for(File file : folder.listFiles()) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            PEMParser pemParser = new PEMParser(br);
            PEMKeyPair key = (PEMKeyPair) pemParser.readObject();
            assertArrayEquals(keys.get(file.getName().split("\\.")[0]).getEncoded(), key.getPrivateKeyInfo().getEncoded());
        }
    }

    @Test
    public void testExtractCerts() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        KeyStore keyStore = setupKeystore();
        HashMap<String, Certificate> certs = TlsHelper.extractCerts(keyStore);
        assertEquals(2, certs.size());
        certs.forEach((String p, Certificate q) -> assertEquals("X.509", q.getType()));
    }

    @Test
    public void testExtractKeys() throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException {
        KeyStore keyStore = setupKeystore();
        HashMap<String, Key> keys = TlsHelper.extractKeys(keyStore, password.toCharArray());
        assertEquals(1, keys.size());
        keys.forEach((String alias, Key key) -> assertEquals("PKCS#8", key.getFormat()));
    }


}
