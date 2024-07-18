package com.example.templates;

public class Credentials {
    //JDBC connection
    public static final String USERNAME = "testuser";
    public static final String PASSWORD = "password";
    public static final String URL = "jdbc:postgresql://localhost:5432/FSAD2024_Records";
    //Client-server connection
    public static final String HOST = "127.0.0.1"; //localhost
    public static final int PORT = 9994; //This is NOT the port in postgres, but the port at which the com.example.templates.RecordsDatabaseServer is listening
}
