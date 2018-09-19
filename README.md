# README #

This is a python program to fetch Double click bid manager data thru the API and insert into Redshift database 

### What is this repository for? ###

* Python Script for Double Click Bid Manger datasource for OTVReports
* v.01


### How do I get set up? ###

* Json Config File.: config.json.dbm.otv

* S3 Folder : buckets3. Rest of the folders are created automatially based on dates

* Environment Variables to be defines: 
	startDATE (Optional)  (From when to fetch the data) (If you dont supply it calculates as yesterday or based on past runs)
	endDATE (Optional)    (Till when to fetch the data) (If you dont supply it calculates as yesterday)

* Redshift Tables: Database  
	Digital.Client_DBM_OTV_Master
	Digital.Client_DBM_OTV_Stg
	Coulmns in both the tables are same


### Who do I talk to? ###

Send a mail to Harmeet for issues
