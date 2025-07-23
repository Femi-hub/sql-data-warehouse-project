🏗️ Building a Data Warehouse with SQL Server

🔍 Project Overview
This project demonstrates how to build a modern data warehouse using SQL Server, covering the full data pipeline from raw ingestion to actionable insights. It includes:
•	ETL Processes using T-SQL procedures
•	Data Modeling (Star Schema with Fact and Dimension tables)
•	Data Analytics with SQL queries for reporting and dashboarding
The goal is to simulate a real-world BI solution that empowers business stakeholders with reliable and accessible data.
________________________________________
📁 Project Structure
bash
CopyEdit
data-warehouse-sql-server/
│
├── scripts/
│   ├── create_database.sql
│   ├── create_schemas.sql
│   ├── create_tables.sql
│   ├── load_bronze.sql
│   ├── load_silver.sql
│   └── analytics_queries.sql
│
├── datasets/
│   ├── customers.csv
│   ├── products.csv
│   ├── sales.csv
│   └── etc.
│
├── diagrams/
│   └── data_model_star_schema.png
│
└── README.md
________________________________________
🔧 Key Components
1. ETL Pipeline
•	Bronze Layer: Raw data staging via BULK INSERT
•	Silver Layer: Cleaned, transformed data with business logic
•	Stored Procedures: Modular ETL logic (load_bronze, load_silver)
2. Data Modeling
•	Star Schema: Includes Fact tables (e.g., fact_sales) and Dimensions (e.g., dim_customer, dim_product)
•	Normalization: Ensures data integrity and efficiency
3. Analytics
•	Sales performance analysis
•	Customer behavior segmentation
•	Product performance tracking
________________________________________
📊 Sample Use Cases
•	Product Performance Dashboard
•	Customer Retention Analysis
•	Sales Trends Over Time
________________________________________
📚 Tools & Technologies
•	SQL Server Management Studio (SSMS)
•	T-SQL
•	Excel, Power BI (for analysis and visualization)
________________________________________
👨🏫 About the Author
Oluwafemi Popoola
Analytics and Data Science Trainer |
Expert in Product Performance Analysis, Customer Analysis & Predictive Modeling
📊 Tools: Excel | Power BI | Tableau | SQL | Python
📫 https://www.linkedin.com/in/oluwafemipopoola | ✉️ oluwafemipopoola00@gmail.com
________________________________________
🚀 Get Started
1.	Clone the repository
git clone https://github.com/Femi-hub/sql-data-warehouse-project.git
2.	Open create_database.sql in SSMS and run step by step
3.	Load the CSVs in datasets/ into the Bronze layer using the provided scripts
4.	Explore insights using analytics_queries.sql
________________________________________
📌 License
This project is licensed under the MIT License.
