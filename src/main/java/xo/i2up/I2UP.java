package xo.i2up;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

public class I2UP {
    private final String caPath;
    private final String baseUrl;
    private final Map<String, String> headers = new HashMap<>();
    private final OkHttpClient httpClient;

    public I2UP(String ip, int port, String caPath, String akPath, String user, String pwd) throws Exception {
        this.caPath = caPath;
        this.baseUrl = String.format("https://%s:%d/api", host2Ip(ip), port);
        this.headers.put("Content-Type", "application/json");

        this.httpClient = getHttpClient();

        if (akPath != null) {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(akPath)) {
                assert is != null;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                    this.headers.put("ACCESS-KEY", br.readLine().trim());
                }
            }
        } else if (user != null && pwd != null) {
            String url = String.format("%s/auth/token", this.baseUrl);
            JSONObject payload = new JSONObject();
            payload.put("username", user);
            payload.put("pwd", pwd);

            String response = post(url, payload.toJSONString());
            JSONObject jsonResponse = JSON.parseObject(response);
            String token = jsonResponse.getJSONObject("data").getString("token");
            this.headers.put("Authorization", token);
        } else {
            throw new IllegalArgumentException("Either akPath or both user and pwd must be provided");
        }
    }

    private String host2Ip(String host) throws UnknownHostException {
        return InetAddress.getByName(host).getHostAddress();
    }

    private OkHttpClient getHttpClient() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate ca;
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(caPath)) {
            assert is != null;
            ca = (X509Certificate) cf.generateCertificate(is);
        }

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", ca);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);

        return new OkHttpClient.Builder()
                .sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) tmf.getTrustManagers()[0])
                .build();
    }

    private String post(String urlString, String payload) throws Exception {
        RequestBody body = RequestBody.create(MediaType.get("application/json; charset=utf-8"), payload);
        Request.Builder requestBuilder = new Request.Builder()
                .url(urlString)
                .post(body);

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }

        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            assert response.body() != null;
            return response.body().string();
        }
    }

    private String get(String urlString) throws Exception {
        Request.Builder requestBuilder = new Request.Builder()
                .url(urlString)
                .get();

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }

        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            assert response.body() != null;
            return response.body().string();
        }
    }

    public String getVersion() throws Exception {
        String url = String.format("%s/version", this.baseUrl);
        String response = get(url);
        JSONObject jsonResponse = JSON.parseObject(response);
        return jsonResponse.getJSONObject("data").getString("version");
    }

    public static void main(String[] args) {
        try {
            I2UP client = new I2UP("centos1", 58086, "ca.crt", "access.key", null,
                    null);
            System.out.println("Version: " + client.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
