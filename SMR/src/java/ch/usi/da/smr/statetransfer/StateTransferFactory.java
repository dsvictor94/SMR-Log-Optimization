package ch.usi.da.smr.statetransfer;

public class StateTransferFactory {

    public static StateTransferInterface getStateTransfer() {
        return getStateTransfer(System.getenv("STATE_TRANSFER"));
    }

    public static StateTransferInterface getStateTransfer(String className) {
        try {
            Class<?> c = Class.forName(className);
            return (StateTransferInterface) c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}