package com.sjyttkl.learning;

import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.util.Random;


public class keyWordTest {


    public static void main(String[] args) throws AWTException {

        Robot robot = new Robot();

        Random random = new Random();

        robot.delay(5000);

        int a = 0;

        while (true) {


            robot.keyPress(KeyEvent.VK_PRINTSCREEN);

            robot.keyRelease(KeyEvent.VK_PRINTSCREEN);

            a = Math.abs(random.nextInt()) % 100 + 50;

            robot.delay(a);


            robot.keyPress(KeyEvent.VK_CONTROL);
            robot.keyPress(KeyEvent.VK_V);

            robot.keyRelease(KeyEvent.VK_CONTROL);
            robot.keyRelease(KeyEvent.VK_V);

            a = Math.abs(random.nextInt()) % 100 + 50;

            robot.delay(a);

            //
            //
            // robot.keyPress(KeyEvent.VK_Q);
            //
            // robot.keyRelease(KeyEvent.VK_Q);
            //
            // a = Math.abs(random.nextInt())%100+50;
            //
            // robot.delay(a);
            //
            //
            //
            // robot.keyPress(KeyEvent.VK_U);
            //
            // robot.keyRelease(KeyEvent.VK_U);
            //
            //
            //
            // robot.keyPress(KeyEvent.VK_SPACE);
            //
            // robot.keyRelease(KeyEvent.VK_SPACE);


            robot.keyPress(KeyEvent.VK_ENTER);

            robot.keyRelease(KeyEvent.VK_ENTER);

            a = Math.abs(random.nextInt()) % 2000 + 1000;

            System.out.println(a);

            robot.delay(a);

        }

    }


    public static void main1111111111(String[] args) throws AWTException {


        Robot robot = new Robot();

        Random random = new Random();

        int a = 0;

        robot.delay(3000);


        robot.mouseMove(1200, 700);

        a = Math.abs(random.nextInt()) % 100 + 50;

        robot.delay(a);


        robot.mousePress(InputEvent.BUTTON1_DOWN_MASK);


        a = Math.abs(random.nextInt()) % 50 + 50;

        robot.delay(a);


        robot.mouseRelease(InputEvent.BUTTON1_DOWN_MASK);

    }


}
