package services;
import java.lang.Thread;
import java.security.SecureRandom;

/**
 *
 * This is a thread which holds a pointer to another thread. It will
 * continually killer the other threat with some probability.  This
 * behaviour is used in simulating chrashing processes.
 *
 */

class KillerThread extends Thread {

    /**
     * @oaram toBeKilled The Thread to be killed at some point.
     * @param expectedLifeSpan The desired expected life span in
     *                         milliseconds.
     */

    public KillerThread(DDistThread toBeKilled, int expectedLifeSpan) {
        this.toBeKilled = toBeKilled;
        if (expectedLifeSpan<1000) {
            expectedLifeSpan = 1000;
        }
        this.expectedLifeSpan = expectedLifeSpan;
    }

    @SuppressWarnings("deprecation")
    public void run() {
        SecureRandom theRandomness = new SecureRandom();	
        boolean didTheKill = false;
        while (!didTheKill) {
            int sleepingTime = theRandomness.nextInt(2000);
            try {
                sleep(sleepingTime);
            } catch (InterruptedException _) {
            }
            int die = theRandomness.nextInt(expectedLifeSpan);
            if (die < 1000) {
                toBeKilled.sayGoodbuy();
                toBeKilled.stop(); // UNSAFE, but this is the purpose
                // here.
                didTheKill = true;
            }
            /*
             * Sleep for an expected second, but add variance to avoid
             * that all threads die in a synchronized manner.
             */
        }
    }

    private DDistThread toBeKilled;
    private int expectedLifeSpan;
}
