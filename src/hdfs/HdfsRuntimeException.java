package hdfs;

public class HdfsRuntimeException extends RuntimeException {
    private static final long serialVersionUID = -2035269340455649839L;

    public HdfsRuntimeException() {
        super();
    }

    public HdfsRuntimeException(String message) {
        super(message);
    }

    public HdfsRuntimeException(Throwable cause) {
        super(cause);
    }
}
