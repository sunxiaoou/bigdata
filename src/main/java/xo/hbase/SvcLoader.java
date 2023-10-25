package xo.hbase;

import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

import java.util.ServiceLoader;

public class SvcLoader
{
    public static void main(String[] args) {
        Iterable<ClientProtocolProvider> frameworkLoader = ServiceLoader.load(ClientProtocolProvider.class);
        for (ClientProtocolProvider provider: frameworkLoader) {
            System.out.println(provider);
        }
    }
}
