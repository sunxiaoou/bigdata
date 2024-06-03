package xo.dumper;

import java.util.concurrent.ThreadLocalRandom;

public class Random {
    public static boolean getRandomBoolean(int denominator) {
        if (denominator <= 0) {
            throw new IllegalArgumentException("Denominator must be greater than 0");
        }
        return ThreadLocalRandom.current().nextInt(denominator) == 0;
    }

    public static void main(String[] args) {
        int denominator = 3; // Change this value to test different probabilities

        // Generate and print a random boolean with the specified probability
        boolean randomBoolean = getRandomBoolean(denominator);
        System.out.println("Random boolean with 1/" + denominator + " probability: " + randomBoolean);
    }
}
