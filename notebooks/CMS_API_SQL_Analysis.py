# %%
# CMS Hospital Readmissions API Data Analysis
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
# ## Business Question 1: What is the distribution of excess readmission ratios by state?

# %%
# SQL query using aggregate functions and GROUP BY
sql_query_1 = '''
SELECT 
    state,
    COUNT(facility_id) AS facility_count,
    AVG(CAST(excess_readmission_ratio AS float)) AS avg_excess_ratio,
    MIN(CAST(excess_readmission_ratio AS float)) AS min_excess_ratio,
    MAX(CAST(excess_readmission_ratio AS float)) AS max_excess_ratio,
    STDDEV(CAST(excess_readmission_ratio AS float)) AS stddev_excess_ratio
FROM 
    raw.cms_hospital_readmissions
WHERE 
    excess_readmission_ratio ~ '^[0-9]*[.][0-9]*$' -- Fixed escape sequence
GROUP BY 
    state
ORDER BY 
    avg_excess_ratio DESC
'''

# Execute query and load results
state_readmission_ratios = pd.read_sql(sql_query_1, engine)

# Display the results
print(f"Analysis of {len(state_readmission_ratios)} states' readmission ratios")
state_readmission_ratios.head(15)

# %%
# Visualize top 10 and bottom 10 states by average excess ratio
plt.figure(figsize=(14, 8))

# Create a horizontal bar plot for better readability with state names
plt.subplot(1, 2, 1)
top_states = state_readmission_ratios.head(10).sort_values('avg_excess_ratio', ascending=False)
sns.barplot(y='state', x='avg_excess_ratio', data=top_states)
plt.title('Top 10 States by Excess Readmission Ratio')
plt.xlabel('Average Excess Ratio')
plt.ylabel('State')
plt.axvline(x=1.0, color='red', linestyle='--', label='Benchmark (1.0)')
plt.legend()

plt.subplot(1, 2, 2)
bottom_states = state_readmission_ratios.tail(10).sort_values('avg_excess_ratio', ascending=False)
sns.barplot(y='state', x='avg_excess_ratio', data=bottom_states)
plt.title('Bottom 10 States by Excess Readmission Ratio')
plt.xlabel('Average Excess Ratio')
plt.ylabel('State')
plt.axvline(x=1.0, color='red', linestyle='--', label='Benchmark (1.0)')
plt.legend()

plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# The analysis reveals significant variation in excess readmission ratios across states. States like Massachusetts, New Jersey, and Illinois have the highest ratios (>1.02), indicating their hospitals have more readmissions than expected. An excess ratio above 1.0 suggests opportunities for improvement in discharge planning and post-acute care coordination.

# %% [markdown]
# ### Recommendation:
# Healthcare systems in high-ratio states should implement targeted readmission reduction programs focusing on care transitions and post-discharge follow-up. CMS could consider directing additional resources to hospitals in these states to help address their disproportionately high readmission rates.

# %% [markdown]
# ## Business Question 2: How do predicted and expected readmission rates compare across different measures?

# %%
# SQL query using CTEs and window functions
sql_query_2 = '''
WITH measure_stats AS (
    SELECT 
        measure_name,
        AVG(CAST(predicted_readmission_rate AS float)) AS avg_predicted_rate,
        AVG(CAST(expected_readmission_rate AS float)) AS avg_expected_rate,
        COUNT(facility_id) AS facility_count
    FROM 
        raw.cms_hospital_readmissions
    WHERE 
        predicted_readmission_rate ~ '^[0-9]*[.][0-9]*$' AND
        expected_readmission_rate ~ '^[0-9]*[.][0-9]*$'
    GROUP BY 
        measure_name
),
ranked_measures AS (
    SELECT 
        measure_name,
        avg_predicted_rate,
        avg_expected_rate,
        avg_predicted_rate - avg_expected_rate AS rate_difference,
        facility_count,
        RANK() OVER (ORDER BY avg_predicted_rate - avg_expected_rate DESC) AS difference_rank
    FROM 
        measure_stats
)
SELECT 
    measure_name,
    avg_predicted_rate,
    avg_expected_rate,
    rate_difference,
    facility_count,
    difference_rank
FROM 
    ranked_measures
ORDER BY 
    difference_rank
'''

# Execute query and load results
measure_comparison = pd.read_sql(sql_query_2, engine)

# Display the results
print(f"Analysis of {len(measure_comparison)} readmission measures")
measure_comparison

# %%
# Visualize the comparison of predicted vs expected rates by measure
plt.figure(figsize=(12, 10))

# Create a plot for the rate differences
plt.subplot(2, 1, 2)
colors = ['red' if x > 0 else 'green' for x in measure_comparison['rate_difference']]
plt.bar(measure_comparison['measure_name'], measure_comparison['rate_difference'], color=colors)
plt.axhline(y=0, color='black', linestyle='-')
plt.title('Difference Between Predicted and Expected Rates (Predicted - Expected)')
plt.xlabel('Measure')
plt.ylabel('Rate Difference (percentage points)')
plt.xticks(rotation=45, ha='right')

# Add annotations for each bar
for i, v in enumerate(measure_comparison['rate_difference']):
    plt.text(i, v + 0.001 if v > 0 else v - 0.01, 
             f"{v:.3f}", ha='center', fontweight='bold')

plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# Heart attack (AMI) and COPD readmissions show the largest positive differences between predicted and expected readmission rates, indicating underperformance relative to expectations. A positive difference suggests hospitals are having more readmissions than expected given their patient population, while negative differences would indicate better-than-expected performance.

# %% [markdown]
# ### Recommendation:
# Healthcare organizations should prioritize intervention programs for conditions with the largest gaps (AMI and COPD), focusing on improved discharge planning, medication reconciliation, and post-acute care coordination specifically for these conditions. Additionally, hospitals performing well on specific measures could share best practices to help address the broader variation in performance.

# %%
# Close database connection
engine.dispose()
print("Database connection closed.")