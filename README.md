# renewable_energy

below the the break down about the test



# 1) Cleans the data: The raw data contains missing values and outliers, which must be removed or imputed.

### drona to remove the NA or NULL values 
`data.dropna()`

### drop duplicates
`data.dropDuplicates(["timestamp", "turbine_id"])`

### Outlier are removed IQR method 
    `https://medium.com/@pp1222001/outlier-detection-and-removal-using-the-iqr-method-6fab2954315d`
    quantiles = data.approxQuantile(["wind_speed", "power_output"], [0.25, 0.75], 0.05)
    Q1_wind_speed, Q3_wind_speed = quantiles[0]
    Q1_power_output, Q3_power_output = quantiles[1]
    IQR_wind_speed = Q3_wind_speed - Q1_wind_speed
    IQR_power_output = Q3_power_output - Q1_power_output
    
    lower_bound_wind_speed = Q1_wind_speed - 1.5 * IQR_wind_speed
    upper_bound_wind_speed = Q3_wind_speed + 1.5 * IQR_wind_speed
    lower_bound_power_output = Q1_power_output - 1.5 * IQR_power_output
    upper_bound_power_output = Q3_power_output + 1.5 * IQR_power_output

    logic to get outliers : 
    (~col("wind_speed").between(lower_bound_wind_speed, upper_bound_wind_speed)) |   (~col("power_output").between(lower_bound_power_output, upper_bound_power_output))`


# 2) Calculates summary statistics: For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
        data.groupBy("event_date", "turbine_id").agg(
        min("power_output").alias("min_power_output"),
        max("power_output").alias("max_power_output"),
        avg("power_output").alias("avg_power_output")

# 3) Identifies anomalies: Identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
    
### Z -Score to get the anomalies    
    `https://medium.com/@datasciencejourney100_83560/z-score-to-identify-and-remove-outliers-c17382a4a739`
    data.groupBy("event_date", "turbine_id").agg(
        avg("power_output").alias("avg_power_output")
    )
    data.agg(mean("avg_power_output")).collect()[0][0]
    data.agg(stddev("avg_power_output")).collect()[0][0]

    logic anomalies: 
    (col("avg_power_output") < overall_avg - 2 * overall_stddev) | (col("avg_power_output") > overall_avg + 2 * overall_stddev)`

# 4) Stores the processed data: Store the cleaned data and summary statistics in a database for further analysis.
  
  `df.write.mode("overwrite").saveAsTable("summary_stats")
  
  df.write.mode("overwrite").saveAsTable("cleaned_data")`

# 5) images screenshot for the proof
	![Alt text](test_1.JPG)
	![Alt text](test_2.JPG)
	![Alt text](test_3.JPG)
	![Alt text](test_4.JPG)
	![image](https://github.com/user-attachments/assets/1e816ffb-40cf-44c1-8166-091c42e68402)
	![image](https://github.com/user-attachments/assets/31c6d5fd-8d34-4c58-983b-4ec21578ca8d)  
  
