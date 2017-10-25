package com.sdu.spark.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class TaskCompletionListenerException extends RuntimeException {

    private List<String> errorMessages;
    private Throwable previousError;

    public TaskCompletionListenerException(List<String> errorMessages) {
        this(errorMessages, null);
    }

    public TaskCompletionListenerException(List<String> errorMessages,
                                           Throwable previousError) {
        this.errorMessages = errorMessages;
        this.previousError = previousError;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        if (errorMessages.size() == 1) {
            sb.append(errorMessages.get(0));
        } else {
            for (int i = 0; i < errorMessages.size(); ++i) {
                sb.append(String.format("Exception %d : %s\n", i, errorMessages.get(i)));
            }
        }

        if (previousError != null) {
            sb.append(String.format("\n\nPrevious exception in task: %s \n%s",
                                    previousError.getMessage(), StringUtils.join(previousError.getStackTrace(), "\n\t")));
        }
        return sb.toString();
    }
}
