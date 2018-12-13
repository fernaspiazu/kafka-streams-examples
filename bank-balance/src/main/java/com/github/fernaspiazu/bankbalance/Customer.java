package com.github.fernaspiazu.bankbalance;

public class Customer {
    private final String name;
    private final Integer amount;
    private final String time;

    Customer(String name, Integer amount, String time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public Integer getAmount() {
        return amount;
    }

    public String getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "Customer{" +
            "name='" + name + '\'' +
            ", amount=" + amount +
            ", time='" + time + '\'' +
            '}';
    }
}
