package xo.dumper;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static xo.dumper.RuleCommand.*;
import static xo.dumper.RuleExceptionNumber.*;

@ChannelHandler.Sharable
public class BackupSvrHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(BackupSvrHandler.class);

    private final boolean receiveCmdFromHelper;
    private final Backup backup;

    public BackupSvrHandler(boolean receiveCmdFromHelper) {
        this.backup = new Backup();
        this.receiveCmdFromHelper = receiveCmdFromHelper;
        LOG.info("receiveCmdFromHelper({})", receiveCmdFromHelper);
    }

    private List<String> getOperationCmds() {
        return Arrays.asList(
                IPC_RULE_ADD,
                IPC_RULE_DEL,
                IPC_RULE_START,
                IPC_RULE_RESUME,
                IPC_RULE_STOP,
                IPC_RULE_CHECK_TOTAL,
                IPC_RULE_CHECK_DONE);
    }

    protected Pair<String, String> parsePacketCmd(Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        try {
            // head1:4bytes
            buf.readInt();

            // head2: 3bytes (length)
            byte[] lenBytes = new byte[3];
            buf.readBytes(lenBytes);
            int length = Utils.convertByteToInt(lenBytes);
            // head3: 1byte (type)
            byte b = buf.readByte();

            // 高总确认，只有等于0需要回复 (其他类型都不需要回复)
            if (0 != b) {
                if (3 == b) {
                    int readableBytes = buf.readableBytes();
                    byte[] content = new byte[readableBytes];
                    buf.readBytes(content);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("this msg may be used to test socket,ignore it. msg body length[{}]", length);
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("type {}", b);
                    }
                }
                return null;
            }

            byte[] cmdBytes = new byte[32];
            buf.readBytes(cmdBytes);
            String cmd = new String(cmdBytes, StandardCharsets.UTF_8);
            cmd = cmd.trim();
            boolean operationCmd = getOperationCmds().contains(cmd);
            if (LOG.isDebugEnabled() && operationCmd) {
                LOG.debug("the command is {}", cmd);
            } else  if (LOG.isTraceEnabled()) {
                LOG.trace("the command is {}", cmd);
            }

            int readableBytes = buf.readableBytes();
            String body = "";
            if (readableBytes > 0) {
                byte[] bytes = new byte[readableBytes];
                buf.readBytes(bytes);
                body = new String(bytes, StandardCharsets.UTF_8);
            }
            body = body.trim();
            if (LOG.isDebugEnabled() && operationCmd) {
                LOG.debug("received body msg:{}", body);
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("received body msg:{}", body);
            }
            return new Pair<>(cmd, body);
        } finally {
            ReferenceCountUtil.release(buf);
        }
    }

    protected void send(AtomicBoolean sent, ChannelHandlerContext ctx, int ack, int code, byte[] buf) {
        if (sent.get()) {
            return;
        }
        sent.set(true);

        ByteBuf byteBuf = Unpooled.buffer(4);
        //ack
        byteBuf.writeInt(ack);
        //len
        int len = (buf == null) ? 4 : (buf.length + 4);
        byteBuf.writeInt(len << 8);
        //code
        byteBuf.writeInt(code);
        //buf
        if (buf != null) {
            byteBuf.writeBytes(buf);
        }

        if (buf != null && LOG.isTraceEnabled()) {
            LOG.trace("send response {}", new String(buf, StandardCharsets.UTF_8));
        }

        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.release();
        try {
            ctx.writeAndFlush(bytes);
            if (LOG.isTraceEnabled()) {
                LOG.trace("send {}", new String(buf, StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }
    }

    private void addRule(String body) {
        LOG.info("RuleManager.addRule({}})", body);
    }

    private void deleteRule(String body) {
        LOG.info("RuleManager.deleteRule({}})", body);
    }

    private void startRule(String body) throws InterruptedException {
        LOG.info("startRule({}})", body);
        backup.resetRuleState();
        backup.start();
    }

    private void resumeRule(String body) {
        LOG.info("RuleManager.resumeRule({}})", body);
    }

    private void stopRule(String body) {
        LOG.info("RuleManager.stopRule({}})", body);
        backup.stop(true);
    }

    private List<String> checkRule(String body) {
        LOG.info("RuleManager.checkRule({})", body);
        return null;
    }

    private List<String> checkRuleDone(String body) {
        LOG.info("RuleManager.checkRuleDone({})", body);
        return null;
    }

    private String tableListToJsonString(List<String> schemaTables) {
        JSONObject checkJson = new JSONObject();
        JSONArray tables = new JSONArray();
        tables.addAll(schemaTables);
        checkJson.put("tabs", tables);
        return checkJson.toJSONString();
    }

    private String getRuleState(String body, boolean flag) {
        LOG.info("RuleManager.getRuleState({})", body);
        return "";
    }

    private String listRuleStates(boolean receiveCmdFromHelper, boolean flag) {
        LOG.info("RuleManager.listRuleStates({}, {})", receiveCmdFromHelper, flag);
        return "";
    }

    private String getVersion() {
        LOG.info("getVersion()");
        return "";
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Pair<String, String> pair = parsePacketCmd(msg);
        if (Objects.isNull(pair)) {
            return;
        }

        AtomicBoolean sent = new AtomicBoolean(false);
        try {
            String cmd = pair.getFirst();
            String body = pair.getSecond();
            switch (cmd) {
                case IPC_RULE_ADD:
                    addRule(body);
                    send(sent, ctx, 0, 0, cmd.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_DEL:
                    deleteRule(body);
                    send(sent, ctx, 0, 0, cmd.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_START:
                    startRule(body);
                    send(sent, ctx, 0, 0, cmd.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_RESUME:
                    resumeRule(body);
                    send(sent, ctx, 0, 0, cmd.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_STOP:
                    stopRule(body);
                    send(sent, ctx, 0, 0, cmd.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_CHECK_TOTAL:
                    List<String> schemaTables = checkRule(body);
                    send(sent, ctx, 0, 0, tableListToJsonString(schemaTables).getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_CHECK_DONE:
                    List<String> schemaTablesDone = checkRuleDone(body);
                    send(sent, ctx, 0, 0, tableListToJsonString(schemaTablesDone).getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_RULE_STATE:
                case IPC_PRINT_SYNC:
                    String ruleState = getRuleState(body, true);
                    send(sent, ctx, 0, 0, ruleState.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_LIST_SYNC:
                    String ruleStates = listRuleStates(receiveCmdFromHelper, true);
                    send(sent, ctx, 0, 0, ruleStates.getBytes(StandardCharsets.UTF_8));
                    break;
                case IPC_GET_VER:
                    send(sent, ctx, 0, 0, getVersion().getBytes(StandardCharsets.UTF_8));
                    break;
                default:
                    LOG.warn("unknown cmd {}", cmd);
                    send(sent, ctx, 0, INVALID_COMMAND, ArrayUtils.add("unknown cmd".getBytes(StandardCharsets.UTF_8), (byte) 0));
            }
        } catch (RuleException e) {
            LOG.error("msg: {}, rule exception: {}", ((ByteBuf) msg).toString(StandardCharsets.UTF_8), e.getLocalizedMessage());
            send(sent, ctx, 0, e.getCode(), ArrayUtils.add(("exception info: " + e.getMessage()).getBytes(StandardCharsets.UTF_8), (byte) 0));
        } catch (JSONException e) {
            LOG.error("msg: {}, failed to parse command content", ((ByteBuf) msg).toString(StandardCharsets.UTF_8), ExceptionUtils.getStackTrace(e));
            send(sent, ctx, 0, INVALID_COMMAND_CONTENT, ArrayUtils.add(("exception info:" + e.getMessage()).getBytes(StandardCharsets.UTF_8), (byte) 0));
        } catch (Exception e) {
            LOG.error("msg: {}, unexpected exception {}", ((ByteBuf) msg).toString(StandardCharsets.UTF_8), ExceptionUtils.getStackTrace(e));
            send(sent, ctx, 0, INTERNAL_ERROR, ArrayUtils.add(("exception info: "+e.getMessage()).getBytes(StandardCharsets.UTF_8), (byte) 0));
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
