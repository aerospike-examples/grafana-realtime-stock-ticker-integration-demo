/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package com.aerospike.app.PopulateStreamingTicks;

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

class tickRange {
    String symbol;
    int lowValue;
    int highValue;
    int tickPctRange;
    double lastTick;
}
   
public class PopulateStreamingTicks {
	
        public static void main(String[] args) throws Exception {
  
        // Connect to Aerospike. Change the IP address as appropriate
	AerospikeClient client = new AerospikeClient("192.168.1.25", 3000);
        WritePolicy policy = new WritePolicy();
        
        // Since some of the records written are fairly large (and I had a WIFI connection), need to change the timeouts from the default
        policy.totalTimeout = 10000;
        policy.socketTimeout = 7500;
        
        System.out.println("Connected to Aerospike");
        
        // Set the ListPolicy so that the list or lists will be ordered
        ListPolicy lpolicy = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.DEFAULT);
        
        // Read from a csv file the parameters for the generation of real-time tick data
        // A sample of the file:
        // Symbol,Min,Max,PctTickChange
        // IBM,130,145,20
        // WMT,101,120,20
        // XOM,65,75,20
        
        String csvFile = "/Users/reuven/TestData/LiveFeed.csv";

        String cvsSplitBy = ",";
        BufferedReader br = null;
        String[] tickerInfo = new String[0];
        ArrayList<tickRange> tickerList = new ArrayList<tickRange>();
        
        try {
            br = new BufferedReader(new FileReader(csvFile));
            br.readLine();
                
            // Get the desired tick parameters for generating data
            String line = "";    
            while ((line = br.readLine()) != null) {
                tickerInfo = line.split(cvsSplitBy);
                tickRange aTickRange = new tickRange();
                aTickRange.symbol = tickerInfo[0];
                aTickRange.lowValue = Integer.valueOf(tickerInfo[1]);
                aTickRange.highValue = Integer.valueOf(tickerInfo[2]);
                aTickRange.tickPctRange = Integer.valueOf(tickerInfo[3]);
                aTickRange.lastTick = aTickRange.lowValue + (aTickRange.highValue - aTickRange.lowValue);
                tickerList.add(aTickRange);
            }
        
            // Now run infinite loop starting from the current time
            // Generate a random tick within the desired range for all symbols, then sleep for a second
            while (true) {
                Long tickMsec = System.currentTimeMillis();
                for (tickRange aTickRange: tickerList) {
                    Vector tickerEntry = new Vector();
                    tickerEntry.add(new LongValue (tickMsec));
                    double dailyHighLowDiff = aTickRange.highValue - aTickRange.lowValue;
                    double tryTick = -1;
                    while (tryTick < aTickRange.lowValue || tryTick > aTickRange.highValue) {
                            tryTick = aTickRange.lastTick - (dailyHighLowDiff * aTickRange.tickPctRange * .01) + ((Math.random()) * 
                                    (dailyHighLowDiff * aTickRange.tickPctRange * 2 * .01));
                    }
                    tickerEntry.add(new StringValue(String.valueOf(tryTick)));
                    
                    aTickRange.lastTick = tryTick;
                    Key key = new Key ("test", "tickerSet", aTickRange.symbol + "-" + new SimpleDateFormat("yyyyMMdd").format(new Date(tickMsec)));
                    client.operate(policy, key, ListOperation.append(lpolicy, "datapoints", new ListValue(tickerEntry)));
                    //System.out.println("Symbol, Value: " + aTickRange.symbol + ", " + aTickRange.lastTick);
                }
                TimeUnit.SECONDS.sleep(1);
               
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
        }
 
}
