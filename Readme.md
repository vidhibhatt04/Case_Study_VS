# Vanilla Steel Case Study

# Setup 
## Database: 
The current pipeline uses locally running postgres database. Also there is additional config to run on Supabase postgres, this was used to load the data in looker studio. 

To test on own installation, make changes to the ```vanilla/profiles.yml ``` file 
## Python environment: 
To create tables and injest data: 

1. Create venv in python 
```
python -m venv venv 
./venv/bin/activate # For Linux/Mac
source venv/Scripts/activate # For Windows

```

2. Install the pip dependencies
```
pip install -r requirements.txt
```

# Task 1

For the task 1, the jupyter notebook is stored under task_1 folder. Execute ```jupyter notebook``` and open the file. The data is stored in the data/task_1 directory.


# Task 2

The deals.csv is used to migrate to a database using the dbt seed method. Execute the below commands:

```
cd vanilla 
dbt seed
dbt run
```
The dbt pipeline creates seeds the deals.csv into a databasse table. The `dbt run` command creates views for account manager insights for the looker dashboard. 

# Task 3

To create the tables in the database, the database table definintion is pre-defined in the file
```
python database_tables_initialize.py
```

To insert the data into the database. This python code reads all the data available, maps it and writes it to the database
```
python data_ingestion_pipeline.py
```

### Future addition of sellers 
To load a new supplier data in future the pipeline is scalable and can be used by following the steps below:
1. Copy the raw file into the data/raw_data folder 
2. Edit the column_mappings_supplier.json: add new column mapping with the file name as the key
```
{
  "your_new_file.csv": {
    "column_1": "mapped_name_1",
    "column_2": "mapped_name_2"
  }
}
```
3. Rerun the data_ingestion_pipeline.py

## Recommendation
To run the recommendation creation in SQL (using dbt)
```
cd vanilla/ # Linux / Mac
dir vanilla/ # Windows

dbt run --models +recommendations           # Generate recommendations
dbt run --models +recommendations_scoring   # Score recommendations
```
The above script creates two tables in the databse: 

**recommendations**: Mathces purely on the basis of Grade or Finish if the Weight and Quantity constrainsts are fulfiled 
 
**recommendations_scoring**: Assigns a matching score by taking into consideration the folowing: Grade, Finish and Thickness 
 
### Testing with DBT
To test the data integreity, currently it checks the buyer_id is not null and the article ID is unique in the buyers and suppliers data respectively.
```
dbt test 
```

### Alternate matching with pandas 
To create score based recommendation table using python and pandas 
```
python recommendation_scoring.py
```

### Evaluation 
The output of recommendation table is exported from postgres for evaluation of the task, it can be found at output_csv/*.csv
```
output_csv/
├── recommendations.csv
└── scored_recommendations.csv
```