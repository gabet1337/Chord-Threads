package services;
import java.lang.Thread;

/**
 *
 * This class behaves exactly as a normal Thread, except that each
 * second it dies with some specified probability. This behaviour is
 * used to simulate chrashing processes.
 */

public class DDistThread extends Thread {

    /**
     * @param expectedLifeSpan The desired expected life span in
     *                         milliseconds. If set to -1, then the 
     *                         Thread is never killed.
     */
    public DDistThread(int expectedLifeSpan) {
        if (expectedLifeSpan != -1) {
            birthMilli = System.currentTimeMillis();
            theKiller = new KillerThread(this, expectedLifeSpan);
            theKiller.start();
        }
    }

    public void sayGoodbuy() {
        System.out.println("Killing thread " + 
                this.getName() + 
                " after " + 
                ((System.currentTimeMillis()-birthMilli)+500)/1000 + 
                " seconds!");
    } 

    private KillerThread theKiller;
    protected long birthMilli;

}
