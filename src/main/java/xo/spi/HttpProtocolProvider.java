package xo.spi;

public class HttpProtocolProvider implements ProtocolProvider {
    public void connect() {
        System.out.println("Connecting via HTTP");
    }
}