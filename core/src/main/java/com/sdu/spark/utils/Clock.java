package com.sdu.spark.utils;

/**
 * @author hanhan.zhang
 * */
public interface Clock {

    long getTimeMillis();
    long waitTillTime(long targetTime);

    class SystemClock implements Clock {

        private long minPollTime = 25L;

        @Override
        public long getTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        public long waitTillTime(long targetTime) {
            long currentTime =  System.currentTimeMillis();

            long waitTime = targetTime - currentTime;
            if (waitTime <= 0) {
                return currentTime;
            }

            long pollTime = Math.max(waitTime / 10, minPollTime);

            while (true) {
                currentTime = System.currentTimeMillis();
                waitTime = targetTime - currentTime;
                if (waitTime <= 0) {
                    return currentTime;
                }

                long sleepTime = Math.min(waitTime, pollTime);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    // ignore
                }

            }
        }

    }
}
