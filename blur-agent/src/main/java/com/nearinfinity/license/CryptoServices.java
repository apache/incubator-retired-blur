package com.nearinfinity.license;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.*;
import javax.crypto.spec.PBEKeySpec;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;

public class CryptoServices {
    public static final String PRIVATE_KEY = "cm_priv";
    public static final String PRIVATE_KEY_FILE = PRIVATE_KEY + ".key";
    public static final String PUBLIC_KEY = "cm_pub";
    public static final String PUBLIC_KEY_FILE = PUBLIC_KEY + ".key";

    public static final String PBE_ALG = "PBEWithSHA1AndDESede";

    public static final String KEY_ALG = "RSA";
    public static final String SIGN_ALG = "SHA512withRSA";

    public static final String ALG_MODE = "ECB";
    public static final String ALG_PADDING = "PKCS1Padding";


    public static final String CIPHER_ALG = KEY_ALG + "/" + ALG_MODE + "/" + ALG_PADDING;

    private KeyFactory keyFactory;
    private Signature signature;
    private Cipher cipher;

    public static CryptoServices INSTANCE = new CryptoServices();

    public static CryptoServices getCryptoServices() {
        return INSTANCE;
    }

    public String encodeBase64(byte[] b) {
        return new String(Base64.encodeBase64(b));
    }

    public byte[] decodeBase64(String s) {
        return Base64.decodeBase64(s.getBytes());
    }

//    private byte[] encrypt(byte[] data, Key key) throws CryptoServicesException {
//        try {
//            getCipher().init(Cipher.ENCRYPT_MODE, key);
//            return cipher.doFinal(data);
//        } catch (InvalidKeyException e) {
//            throw new CryptoServicesException(e);
//        } catch (BadPaddingException e) {
//            throw new CryptoServicesException(e);
//        } catch (IllegalBlockSizeException e) {
//            throw new CryptoServicesException(e);
//        } catch (NoSuchAlgorithmException e) {
//            throw new CryptoServicesException(e);
//        } catch (NoSuchPaddingException e) {
//            throw new CryptoServicesException(e);
//        }
//    }

    public byte[] decrypt(byte[] data, Key key) throws CryptoServicesException {
        try {
            getCipher().init(Cipher.DECRYPT_MODE, key);
            return cipher.doFinal(data);
        } catch (InvalidKeyException e) {
            throw new CryptoServicesException(e);
        } catch (BadPaddingException e) {
            throw new CryptoServicesException(e);
        } catch (IllegalBlockSizeException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchPaddingException e) {
            throw new CryptoServicesException(e);
        }
    }

    public byte[] sign(byte[] data, PrivateKey key) throws CryptoServicesException {
        try {
            getSignature().initSign(key);
            signature.update(data);
            return signature.sign();
        } catch (InvalidKeyException e) {
            throw new CryptoServicesException(e);
        } catch (SignatureException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        }
    }

    public boolean verify(byte[] data, byte[] sig, PublicKey key) throws CryptoServicesException {
        try {
            getSignature().initVerify(key);
            signature.update(data);
            return signature.verify(sig);
        } catch (InvalidKeyException e) {
            throw new CryptoServicesException(e);
        } catch (SignatureException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        }
    }

    private KeyFactory getKeyFactory() throws NoSuchAlgorithmException {
        if (keyFactory == null) {
            keyFactory = KeyFactory.getInstance(KEY_ALG);
        }
        return keyFactory;
    }

    private Signature getSignature() throws NoSuchAlgorithmException {
        if (signature == null) {
                signature = Signature.getInstance(SIGN_ALG);
        }
        return signature;
    }

    public SecretKey generatePBESecretKey(char[] password, byte[] salt) throws CryptoServicesException {

        PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
        SecretKeyFactory secretKeyFactory = null;
        try {
            secretKeyFactory = SecretKeyFactory.getInstance(PBE_ALG);
            return secretKeyFactory.generateSecret(pbeKeySpec);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        } catch (InvalidKeySpecException e) {
            throw new CryptoServicesException(e);
        }
    }

    private Cipher getCipher() throws NoSuchAlgorithmException, NoSuchPaddingException {
         cipher = Cipher.getInstance(CIPHER_ALG);
        return cipher;
    }

    public final PublicKey getPublicKey(byte[] keyBytes) throws CryptoServicesException {
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(keyBytes);
        PublicKey publicKey = null;
        try {
            publicKey = getKeyFactory().generatePublic(publicKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        }
        return publicKey;

    }


    public final PrivateKey getPrivateKey(byte[] keyBytes) throws CryptoServicesException {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(keyBytes);
        PrivateKey privateKey = null;
        try {
            privateKey = getKeyFactory().generatePrivate(privateKeySpec);
        } catch (InvalidKeySpecException e) {
            throw new CryptoServicesException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new CryptoServicesException(e);
        }
        return privateKey;

    }


    public KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
        KeyPairGenerator kpg = KeyPairGenerator.getInstance(KEY_ALG);
        kpg.initialize(4096, secureRandom);

        KeyPair keyPair = kpg.generateKeyPair();

        return keyPair;
    }
}
