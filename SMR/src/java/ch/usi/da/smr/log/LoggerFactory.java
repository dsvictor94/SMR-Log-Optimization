package ch.usi.da.smr.log;

public class LoggerFactory {

    public static LoggerInterface getLogger() {
        return getLogger(System.getenv("APPLICATION_LOGGER"));
    }

    public static LoggerInterface getLogger(String className) {
        try {
            Class<?> c = Class.forName(className);
            return (LoggerInterface) c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}