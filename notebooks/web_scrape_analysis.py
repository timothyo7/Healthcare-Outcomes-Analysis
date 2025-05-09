# %%
# Hospital Infections Analysis
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# %%
# Load environment variables and create database connection
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Test database connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")

# %% [markdown]
# ## Business Question 1: How do hospital ownership types differ in infection rates, and could this impact readmission patterns?

# %%
# SQL query using aggregate functions, GROUP BY and JOIN
sql_query_1 = '''
WITH ownership_infection_stats AS (
    SELECT
        hp.ownership_type,
        COUNT(DISTINCT hp.facility_name) AS facility_count,
        AVG(CAST(hi.indicator_value AS float)) AS avg_infection_rate,
        AVG(CAST(hi.infections_observed AS float)) AS avg_infections_observed,
        STDDEV(CAST(hi.indicator_value AS float)) AS stddev_infection_rate
    FROM 
        raw.ny_hospital_profiles hp
    JOIN 
        raw.ny_hospital_infections hi ON hp.facility_name = hi.hospital_name
    WHERE 
        hi.indicator_value ~ '^[0-9]*[.][0-9]*$'
    GROUP BY 
        hp.ownership_type
)
SELECT * FROM ownership_infection_stats
ORDER BY avg_infection_rate DESC
'''

# Execute query and load results
ownership_infection_stats = pd.read_sql(sql_query_1, engine)

# Display the results
print(f"Analysis of infection rates by {len(ownership_infection_stats)} ownership types")
ownership_infection_stats

# %%
# Visualize ownership types by infection rates
plt.figure(figsize=(12, 8))

# Create bar plot showing facility count by ownership type
plt.subplot(2, 1, 1)
sns.barplot(y='ownership_type', x='facility_count', data=ownership_infection_stats)
plt.title('Number of Facilities by Ownership Type')
plt.xlabel('Number of Facilities')
plt.ylabel('Ownership Type')

# Create a bar plot
plt.subplot(2, 1, 2)
sns.barplot(y='ownership_type', x='avg_infection_rate', data=ownership_infection_stats)
plt.title('Average Infection Rate by Hospital Ownership Type')
plt.xlabel('Average Infection Rate')
plt.ylabel('Ownership Type')

plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# The data reveals consistent differences in infection rates across hospital ownership types, with County-owned facilities showing the lowest average rate (1.53) while Municipality-owned facilities have the highest (1.94). Despite having the largest sample size (144 facilities), Not for Profit Corporations maintain a moderate infection rate (1.77), though they show significant variation in performance as seen in earlier data on top-performing hospitals. Notably, County facilities show the lowest average observed infections (0.88) but the highest standard deviation (7.52), suggesting inconsistent performance across this small group.

# %% [markdown]
# ### Recommendation:
# Develop targeted infection control protocols based on ownership type, prioritizing knowledge transfer from high-performing County facilities to Municipality-owned hospitals with systematically higher rates. Investigate the relationship between infection rates and readmission patterns across ownership categories to identify whether improvements in infection control translate to reduced readmissions, particularly within the Not for Profit sector where the large sample size (144 facilities) provides robust data for establishing best practices.

# %% [markdown]
# ## Business Question 2: Which hospitals have shown significant improvements in infection rates over time, and could their strategies help reduce readmissions?

# %%
# SQL query using window functions to analyze year-over-year improvements
sql_query_2 = '''
WITH yearly_hospital_rates AS (
    SELECT
        hospital_name,
        year,
        AVG(CAST(indicator_value AS float)) AS avg_infection_rate,
        SUM(CAST(infections_observed AS float)) AS total_infections
    FROM 
        raw.ny_hospital_infections
    WHERE 
        indicator_value ~ '^[0-9]*[.][0-9]*$'
        AND year IN ('2020', '2021', '2022')
    GROUP BY 
        hospital_name, year
),
hospital_improvements AS (
    SELECT 
        hospital_name,
        AVG(avg_infection_rate) AS three_year_avg_rate,
        MAX(CASE WHEN year = '2020' THEN avg_infection_rate ELSE NULL END) AS rate_2020,
        MAX(CASE WHEN year = '2021' THEN avg_infection_rate ELSE NULL END) AS rate_2021,
        MAX(CASE WHEN year = '2022' THEN avg_infection_rate ELSE NULL END) AS rate_2022,
        (MAX(CASE WHEN year = '2020' THEN avg_infection_rate ELSE NULL END) - 
         MAX(CASE WHEN year = '2022' THEN avg_infection_rate ELSE NULL END)) AS improvement_2020_to_2022,
        SUM(total_infections) AS total_infections_2020_2022
    FROM 
        yearly_hospital_rates
    GROUP BY 
        hospital_name
    HAVING 
        COUNT(DISTINCT year) = 3  -- Ensure data for all three years
)
SELECT 
    h.*,
    hp.ownership_type,
    hp.county
FROM 
    hospital_improvements h
JOIN 
    raw.ny_hospital_profiles hp ON h.hospital_name = hp.facility_name
WHERE 
    h.improvement_2020_to_2022 IS NOT NULL
ORDER BY 
    h.improvement_2020_to_2022 DESC
LIMIT 20
'''

# Execute query and load results
hospital_improvements = pd.read_sql(sql_query_2, engine)

# Display the results
print(f"Analysis of top {len(hospital_improvements)} hospitals with largest infection rate improvements")
hospital_improvements.head(10)

# %%
# Visualize top improving hospitals
plt.figure(figsize=(14, 10))

# Select top 10 hospitals with the most improvement
top_improving = hospital_improvements.head(10)

# Create a horizontal bar chart showing improvement
plt.subplot(2, 1, 1)
improvement_plot = sns.barplot(y='hospital_name', x='improvement_2020_to_2022', data=top_improving)
plt.title('Top 10 Hospitals with Greatest Infection Rate Improvement (2020-2022)')
plt.xlabel('Improvement in Infection Rate')
plt.ylabel('Hospital')

# Add value labels
for i, v in enumerate(top_improving['improvement_2020_to_2022']):
    improvement_plot.text(v + 0.05, i, f"{v:.2f}", va='center')

# Create line chart showing trends for top 5 hospitals
plt.subplot(2, 1, 2)
top5 = top_improving.head(5)
for _, row in top5.iterrows():
    plt.plot([2020, 2021, 2022], [row['rate_2020'], row['rate_2021'], row['rate_2022']], 
             marker='o', linewidth=2, label=row['hospital_name'])

plt.title('Infection Rate Trends of Top 5 Most Improved Hospitals (2020-2022)')
plt.xlabel('Year')
plt.ylabel('Infection Rate')
plt.legend(loc='best')
plt.grid(True, linestyle='--', alpha=0.7)

plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# The data reveals significant infection rate reductions among several hospitals between 2020-2022, with Massena Hospital achieving the most dramatic improvement (29.44 points, from 31.24 to 1.79). The most substantial reductions occurred between 2020-2021, with most facilities maintaining their improvements through 2022. These top-performing hospitals, all not-for-profit organizations, demonstrate that significant infection control improvements are achievable in a relatively short timeframe.

# %% [markdown]
# ### Recommendation:
# Implement a structured knowledge transfer program to study infection control protocols from Massena Hospital and Montefiore Mount Vernon Hospital, which achieved 94% and 73% reductions respectively. Analyze whether these infection control improvements correlate with corresponding reductions in readmission rates, particularly for infection-related conditions, to develop targeted intervention strategies that address both metrics simultaneously.

# %%
# Close database connection
engine.dispose()
print("Database connection closed.")