package com.example.sparkexcercise;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

public class IntegerWithSquare {
    private int originalNumber;
    private double squareRoot;

    public IntegerWithSquare(int originalNumber) {
        this.originalNumber = originalNumber;
        this.squareRoot = Math.sqrt(originalNumber);
    }
}
