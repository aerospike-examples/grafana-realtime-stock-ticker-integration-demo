# grafana-realtime-stock-ticker-integration-demo
Demo of realtime integration between Aerospike and Grafana with realtime stock ticker data being added and retrieved from Aerospike.

Included in this project are:
- Screen capture of Grafana displaying tick data for 2 symbols for 5 minutes with a 5 second refresh
- The Java source, PopulateTimeStampList.java which will populate the Aerospike DB based on input CSV files (with randomization to create more detailed data) as downloaded directly from Yahoo Finance
- The Java source, PopulateStreamingTicks.java, which adds new random tick data to the Aerospike DB every second.
- The Grafana datasource, written in Python that feeds the Grafana application from the Aerospike DB
