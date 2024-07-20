package com.batch.csv_to_mysql.entities;

public class User {

    private String userId;
    private String firstName;
    private String lastName;
    private String city;


    public User() {
    }

    public User(String userId, String firstName, String lastName, String city) {
        this.userId = userId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.city = city;
    }


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
