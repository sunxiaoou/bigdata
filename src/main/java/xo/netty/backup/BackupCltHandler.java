package xo.netty.backup;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

class BackupCltHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(BackupClient.class);

    private byte[] reqJson;

    public BackupCltHandler(String cmd, String jsonStr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("cmd: {}, jsonStr: {}", cmd, jsonStr);
        }
        if (StringUtils.isBlank(jsonStr)) {
            jsonStr = "";
        }

        byte[] byCmd = cmd.getBytes(StandardCharsets.UTF_8);
        byte[] byJsonStr = jsonStr.getBytes(StandardCharsets.UTF_8);
        reqJson = new byte[32 + byJsonStr.length];
        System.arraycopy(byCmd,0, reqJson,0, byCmd.length);
        System.arraycopy(byJsonStr,0, reqJson,32, byJsonStr.length);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ByteBuf msg = Unpooled.buffer();
        // param1: 4 bytes
        msg.writeInt(0);
        // param2: 3 bytes, cmd+json length
        msg.writeBytes(Utils.convertIntToByte(reqJson.length, 3));
        if (LOG.isDebugEnabled()) {
            LOG.debug("write message header, length:{}", Utils.convertByteToInt(Utils.convertIntToByte(reqJson.length, 3)));
        }
        // param3: 1 byte, no meanings
        msg.writeByte(0);

        msg.writeBytes(reqJson);

//        byte[] token = IPCServerInitializer.getToken();
//        if (Objects.nonNull(token)) {
//            for (int i = 0; i < 16; ++i) {
//                msg.array()[i + 8] ^= token[i];
//                msg.array()[16 + i + 8] ^= token[i];
//            }
//        }
        ctx.writeAndFlush(msg);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        // head1: 4byte
        buf.readInt();
        // head2: 3byte
        byte[] lenByte = new byte[3];
        buf.readBytes(lenByte);
        int length = Utils.convertByteToInt(lenByte);
        // head3:1byte
        buf.readByte();
        // code: 4byte;
        // json-body
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        String body = new String(bytes, StandardCharsets.UTF_8);

        System.out.println("response: \n" + body);

        ctx.close();
    }
}
