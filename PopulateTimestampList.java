/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.aerospike.app.PopulateTimestampList;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.Value.ListValue;
import com.aerospike.client.Value.LongValue;
import com.aerospike.client.Value;
import com.aerospike.client.Value.StringValue;
import java.util.GregorianCalendar;
import java.util.Vector;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author reuven
 */

   
public class PopulateTimestampList {
    
        // The ticker symbols that we will load  
        enum StockSymbols {
            IBM,
            WMT,
            XOM,
            BRK,
            AMZN
        }
	
        public static void main(String[] args) throws Exception {

        // Connect to Aerospike. Change the IP address as appropriate
	AerospikeClient client = new AerospikeClient("192.168.1.25", 3000);
        WritePolicy policy = new WritePolicy();
        // Since some of the records written are fairly large (and I had a WIFI connection), need to change the timeouts from the default
        policy.totalTimeout = 10000;
        policy.socketTimeout = 7500;
        
        // Create a ListPolicy that will force the List of Lists to be ordered by timestamp (the first member of each tick List)
        ListPolicy lpolicy = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT);

        String cvsSplitBy = ",";
        
        // For each of the stock symbols 
        for (StockSymbols aStock: StockSymbols.values()){
            
            // Open up the associated input file
            // The first few records of one of the files, extracted from Yahoo Finance, is as follows:
            //          Date,Open,High,Low,Close,Adj Close,Volume
            //          2014-11-11,163.699997,163.899994,162.600006,163.300003,131.590302,3534400
            //          2014-11-12,162.279999,163.000000,161.759995,161.919998,130.478241,3378200
            //          2014-11-13,162.000000,162.800003,161.800003,162.789993,131.179306,3239700 
            String csvFile = "/Users/reuven/Downloads/" + aStock + ".csv";
            System.out.println("File: " + csvFile);
            BufferedReader br = null;
            String line = "";
            String[] dailyData = new String[0];
            Vector allTickers = new Vector();
            ArrayList<Value> dailyCloses = new ArrayList<Value>();
            int currentYear = -1; // Initialized to a dummy year
                 
            try {
                br = new BufferedReader(new FileReader(csvFile));
                // Skip the header line
                br.readLine();
                
                
                // For each input record
                
                
                while ((line = br.readLine()) != null) {
                    dailyData = line.split(cvsSplitBy);
                    allTickers = new Vector();
                    Vector closeTickers = new Vector();
                    Vector tickerEntry = new Vector();
                    
                    // Create a ListValue for the Daily open price (9:30AM) and the close price (4:00PM) based upon the input data
                    
                    Long tickMsec = new Long(new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(dailyData[0] + " 09:30").getTime());
                    tickerEntry.add(new LongValue(tickMsec)); // Opening Price Time
                    tickerEntry.add(new StringValue(dailyData[1]));
                    //tickerEntry.add(new StringValue(decSplit[1].substring(0, 2))); // Fractional value
                    allTickers.add(new ListValue(tickerEntry));
                    
                    // Add the CLOSE value to a list for the symbol
                    // If there's a change in year, then write the record of CLOSING prices for the year
                    if ((Integer.valueOf(dailyData[0].substring(0,4))) != currentYear) {
                        if (currentYear != -1) {
                            Key key = new Key ("test", "tickerSet", aStock.toString() + "-" + String.valueOf(currentYear));
                            client.operate(policy, key, ListOperation.appendItems(lpolicy, "datapoints", dailyCloses));
                            
                            dailyCloses = new ArrayList();
                        }
                        currentYear = Integer.valueOf(dailyData[0].substring(0,4));
                    }
                    // Create a record for the closing price alone
                    dailyCloses.add(new ListValue(tickerEntry));
                                       
                    // Now need to generate random prices for a List within a single daily range
                    // Open is [1], High is [2], Low is [3], Close is [5]
                    // Open is 9:30AM Close is 4PM
                    // Each second should have a value between the high and low, with a random change of no more than 5% of the difference.
                    double prevTick = new Float(dailyData[1]);
                    float dailyHigh = Float.parseFloat(dailyData[2]);
                    float dailyLow = Float.parseFloat(dailyData[3]);
                    float dailyHighLowDiff = dailyHigh - dailyLow;
                    for (int sec = 1; sec < (6.5 * 60 * 60); sec++) {
                        tickerEntry = new Vector();
                        tickMsec = tickMsec + 1000; //add one second
                        tickerEntry.add(new LongValue(tickMsec));
                        double tryTick = -1;
                        
                        // If the random tick value is less than the daily low or greater than the daily high, try again
                        while (tryTick < dailyLow || tryTick > dailyHigh) {
                            tryTick = prevTick - (dailyHighLowDiff * .05) + ((Math.random()) * (dailyHighLowDiff * .10));
                        }
                        String[] decSplit = String.valueOf(tryTick).split("\\.");

                        //Convert the tick value to a string
                        try {
                            tickerEntry.add(new StringValue(decSplit[0]) + "." + decSplit[1].substring(0,6));
                        } catch (StringIndexOutOfBoundsException e) {
                            tickerEntry.add(new StringValue(String.valueOf(tryTick)));
                        }
                      
                        // Add the timstamp, tick list value to the list of ticks
                        allTickers.add(new ListValue(tickerEntry));
                        prevTick = tryTick;
                        
                    } 
                // Now create a record for the symbol per day with all the ticks in a sorted list
                Key key = new Key ("test", "tickerSet", aStock.toString() + "-" + dailyData[0].replaceAll("-", ""));
                client.operate(policy, key, ListOperation.appendItems(lpolicy, "datapoints", allTickers));
                }
                
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                        try {
                                br.close();
                        } catch (IOException e) {
                                e.printStackTrace();
                        } 
                }
            }
            Key key = new Key ("test", "tickerSet", aStock.toString() + "-" + String.valueOf(currentYear));
            client.operate(policy, key, ListOperation.appendItems(lpolicy, "datapoints", dailyCloses));   
        }
        client.close();
        
        System.out.println("DONE!");
       
        System.exit(0);
               
	}
}
