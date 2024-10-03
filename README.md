# Instacart Analysis Using PySpark
## Overview
This project analyzes the Instacart dataset using PySpark to derive insights about customer purchasing patterns across various aisles and departments. The analysis involves writing SQL queries to answer specific business questions.

## Dataset
The dataset used in this project is provided via Moodle and imported into an SQLite database. The analysis focuses on answering specific questions related to product orders, aisle preferences, and recommendations for improving product placements.

## Analyses Performed
1. **Top 3 Aisles by Product Orders**  
   Query to determine the top 3 aisles with the most products ordered.
   
2. **Mean and Variance of Products per Aisle**  
   Calculation of the mean and variance of the number of products ordered in each aisle.

3. **Co-occurrence of Products with "Speciality Cheeses"**  
   Identification of which aisle’s products are most frequently bought alongside products from the "speciality cheeses" aisle.

4. **Frozen Products and Bakery Products Placement**  
   SQL queries to support or oppose the recommendation to place frozen products next to bakery products based on customer purchasing behavior.

5. **Top 5 Most Reordered Products**  
   Query to list the top 5 products that are most frequently reordered by customers.

6. **Customer Retargeting Campaign Suggestions**  
   Identification of 3 products that should be offered discounts in a customer retargeting campaign.

7. **Departmental Variations by Time of Day**  
   Analysis of whether orders from different departments vary by time of day, identifying morning and evening departments.

8. **Average and Maximum Number of Products per Order**  
   Calculation of the average and maximum number of products per order.

## Tools Used
- **PySpark**: For data processing and analysis.
- **Google Cloud Storage (GCS)**: For storing and retrieving the dataset.
- **SQL**: For writing queries to answer the business questions.
- **Jupyter Notebook**: For running and documenting the queries and analysis.
