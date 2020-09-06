# WebScraping-MoneyContol
This is the simple web scraping ETL project developed using Python3.6, Airflow and Apache Hive.
The following are the key features of this data pipeline.
  1. Index stats extraction from www.moneycontrol.com website
  2. Add detail timestamp part in transformation 
  3. Load data in stage table at hive
  4. Update latest records into the final table at hive
  5. Airflow dag trigger after 20 min to extract data from moneycontrol site. 
 
Tools and Technologies :
1. Coding part : Python3.6
2. Workflow Management : Airflow 1.10.10
3. Storage : Apache Hive
