package xo.spi;

import java.util.ServiceLoader;

public class Main {
    static Iterable<ProtocolProvider> loader = ServiceLoader.load(ProtocolProvider.class);

    public static void main(String[] args) {
        for (ProtocolProvider provider : loader) {
            provider.connect();
        }
    }
}