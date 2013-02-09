package tests;

import services.*;

public class DDistThreadTest extends DDistThread  {

    public DDistThreadTest() {
        super(30000); // Expect to live for 30 seconds
    }

    public void run() {
        // Report that you are alive every 5 seconds
        while (true) {
            System.out.println(getName() + " is still running, happy days :-)");
            try {
                sleep(5000);
            } catch (InterruptedException _) {
            }
        }
    }

    public static void main(String[] _) {
        // Start 10 threads
        for (int i=0; i<10; i++) {
            new DDistThreadTest().start();
        }
    }

}
