/*
 * com.example.templates.RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2598057>
 *
 */

import com.example.templates.Credentials;

import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class RecordsDatabaseService extends Thread {

    private Socket serviceSocket = null;
    private String[] requestStr = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL = Credentials.URL;


    //Class constructor
    public RecordsDatabaseService(Socket aSocket) {
        //TO BE COMPLETED
        serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest() {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop

        String tmp = "";
        try {
            //TO BE COMPLETED
            InputStream streamSocket = this.serviceSocket.getInputStream();
            InputStreamReader readerSocket = new InputStreamReader(streamSocket);
            StringBuffer sbArtist = new StringBuffer();
            StringBuffer sbShop = new StringBuffer();
            boolean done = false;

            char eachChar;
            while (true){
                eachChar = (char) readerSocket.read();
                if (eachChar == '#'){
                    this.requestStr[0] = sbArtist.toString();
                    this.requestStr[1] = sbShop.toString();
                    break;
                }
                else if ((!done) && (eachChar != ';')){
                    sbArtist.append(eachChar);
                }
                else if ((!done) && (eachChar == ';')){
                    done = true;
                }
                else{
                    sbShop.append(eachChar);
                }
            }


        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest() {
        boolean flagRequestAttended = true;

        this.outcome = null;

        String sql = ""; //TO BE COMPLETED- Update this line as needed.


        try {
            //Connet to the database
            //TO BE COMPLETED

            //Make the query
            //TO BE COMPLETED
            Class.forName("org.postgresql.Driver");
            Connection connect = DriverManager.getConnection(URL,USERNAME,PASSWORD);
            sql= """
                                SELECT record.title, record.label,  record.genre,  record.rrp, COUNT(*) AS copies_available
                                FROM record
                                INNER JOIN  artist ON record.artistid = artist.artistid
                                INNER JOIN  recordcopy ON record.recordid = recordcopy.recordid
                                INNER JOIN  recordshop ON recordcopy.recordshopid = recordshop.recordshopid
                                WHERE  artist.lastname = ? AND recordshop.city = ?
                                GROUP BY  record.title,  record.label, record.genre, record.rrp
                                ORDER BY record.title;
                    """;

            PreparedStatement preparedStatement = connect.prepareStatement(sql);
            preparedStatement.setString(1, this.requestStr[0]);
            preparedStatement.setString(2, this.requestStr[1]);

            this.outcome = preparedStatement.executeQuery();
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet cachedRS = aFactory.createCachedRowSet();
            cachedRS.populate(this.outcome);
            cachedRS.beforeFirst();
            System.out.println("outcome = "+ outcome);
            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            while (cachedRS.next()){
                System.out.print(cachedRS.getObject("title") + " | " + cachedRS.getObject("label") + " | " + cachedRS.getObject("genre") + " | " + cachedRS.getObject("rrp") + " | " + cachedRS.getObject("copies_available") + "\n" );
            }
            cachedRS.beforeFirst();
            this.outcome = cachedRS;

            //Clean up
            //TO BE COMPLETED
            preparedStatement.close();
            connect.close();



        } catch (Exception e) {
            System.out.println(e);
            flagRequestAttended = false;
        }

        return flagRequestAttended;
    }


    //Wrap and return service outcome
    public void returnServiceOutcome() {
        try {
            //Return outcome
            //TO BE COMPLETED
            this.outcome.beforeFirst();
            ObjectOutputStream streamOutcome = new ObjectOutputStream(this.serviceSocket.getOutputStream());
            streamOutcome.writeObject(this.outcome);

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            //TO BE COMPLETED
            this.serviceSocket.close();


        } catch (IOException | SQLException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run() {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        } catch (Exception e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
