package cz.scholz.vertxamqpserver;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.proton.*;
import org.apache.qpid.proton.message.Message;

/**
 * Created by schojak on 16.12.16.
 */
public class Server extends AbstractVerticle {
    final static private Logger LOG = LoggerFactory.getLogger(Server.class);

    @Override
    public void start(Future<Void> fut) {
        ProtonServer server = ProtonServer.create(vertx, new ProtonServerOptions().setPort(5672)).connectHandler(this::connectionReceivedHandler).listen();

        fut.complete();
    }

    private void connectionReceivedHandler(ProtonConnection pc) {
        LOG.info("Connection request received");
        pc.openHandler(this::connectionOpenHandler);
        pc.closeHandler(this::connectionCloseHandler);
        pc.receiverOpenHandler(this::connectionReceiverOpenHandler);
        pc.senderOpenHandler(this::connectionSenderOpenHandler);
        pc.sessionOpenHandler(this::connectionSessionOpenHandler);
        pc.disconnectHandler(this::disconnectHandler);
        //pc.open();
    }

    private void disconnectHandler(ProtonConnection protonConnection) {
        LOG.info("Connection disconnected");
        protonConnection.disconnect();
    }

    private void connectionSessionOpenHandler(ProtonSession protonSession) {
        LOG.info("Session open received on connection level");
        protonSession.closeHandler(this::sessionCloseHandler);
        protonSession.openHandler(this::sessionOpenHandler);
        protonSession.open();
    }

    private void sessionOpenHandler(AsyncResult<ProtonSession> protonSessionAsyncResult) {
        LOG.info("Session open received");
        protonSessionAsyncResult.result().open();
    }

    private void sessionCloseHandler(AsyncResult<ProtonSession> protonSessionAsyncResult) {
        LOG.info("Session close received");
        protonSessionAsyncResult.result().close();
    }

    private void connectionReceiverOpenHandler(ProtonReceiver protonReceiver) {
        LOG.info("Receiver received");
        protonReceiver.closeHandler(this::receiverCloseHandler);
        protonReceiver.openHandler(this::receiverOpenHandler);
        protonReceiver.handler(this::receiverMessage);
        protonReceiver.setSource(protonReceiver.getRemoteSource());
        protonReceiver.setTarget(protonReceiver.getRemoteTarget());
        protonReceiver.open();
    }

    private void receiverMessage(ProtonDelivery protonDelivery, Message message) {
        LOG.info("Receiver received a message: ", message);
        protonDelivery.settle();
    }

    private void receiverOpenHandler(AsyncResult<ProtonReceiver> protonReceiverAsyncResult) {
        LOG.info("Receiver open received");
        protonReceiverAsyncResult.result().open();
    }

    private void receiverCloseHandler(AsyncResult<ProtonReceiver> protonReceiverAsyncResult) {
        LOG.info("Receiver close received");
        protonReceiverAsyncResult.result().close();
    }

    private void connectionSenderOpenHandler(ProtonSender protonSender) {
        LOG.info("Sender received");
        protonSender.closeHandler(this::senderCloseHandler);
        protonSender.openHandler(this::senderOpenHandler);
    }

    private void senderCloseHandler(AsyncResult<ProtonSender> protonSenderAsyncResult) {
        LOG.info("Sender close received");
        protonSenderAsyncResult.result().close();
    }

    private void senderOpenHandler(AsyncResult<ProtonSender> protonSenderAsyncResult) {
        LOG.info("Sender open received");
        protonSenderAsyncResult.result().open();
    }

    private void connectionCloseHandler(AsyncResult<ProtonConnection> protonConnectionAsyncResult) {
        LOG.info("Connection close received");
        protonConnectionAsyncResult.result().close();
    }

    private void connectionOpenHandler(AsyncResult<ProtonConnection> protonConnectionAsyncResult) {
        LOG.info("Connection open received");
        protonConnectionAsyncResult.result().open();
    }


    @Override
    public void stop() throws Exception {
        // Nothing
    }
}
