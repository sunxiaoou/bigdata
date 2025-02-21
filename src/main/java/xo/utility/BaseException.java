/***************************************************************************
 Copyright(c) 2019 Information2 Software Inc.
 
 Name: Exception.java
 Description: Base exception class
 
 ****************************************************************************/
// package membership
package xo.utility;

// Standard JDK imports

// Information2 Software imports

/**
 * BaseException is the generic base class for all exception types.
 * 
 * @author nigj
 */
public class BaseException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    // --- Class constants (public first) ---

    // --- Class variables (public first) ---

    // --- Fields (public first) ---
    private String localizedMessage;

    private Object[] params;

    // ---------+---------+---------+---------+---------+---------+---------+---------*

    public BaseException() {
    }

    public BaseException(String message) {
        super(message);
        this.localizedMessage = message;
    }

    public Object[] getParams() {
        return params;
    }

    public void setParams(Object[] params) {
        this.params = params;
    }

    /**
     * Creates a new BaseException with specified message text and cause
     * 
     * @param message Message related to the exception
     * @param cause   Cause can be null or the exception thrown by the inner layers
     */
    public BaseException(String message, Throwable cause) {
        super(message, cause);
        this.localizedMessage = message;
    }

    public void setLocalizedMessage(String localizedMessage) {
        this.localizedMessage = localizedMessage;
    }

    @Override
    public String getLocalizedMessage() {
        return this.localizedMessage;
    }

    // ----- Static methods -----

}
