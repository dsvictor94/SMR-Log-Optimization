package ch.usi.da.smr.statetransfer;

import ch.usi.da.smr.log.LoggerInterface;
import ch.usi.da.smr.message.Message;

class StateTransferDummy implements StateTransferInterface {
    // restaura de si mesmo
    private LoggerInterface logger = null;

    @Override
    public void init(LoggerInterface logger) {
        this.logger = logger;
    }

    @Override
    public Iterable<Message> restore(int ring, long from, long to) {
        return logger.retrive(ring, from, to);
    }

    @Override
    public void close() {}
}