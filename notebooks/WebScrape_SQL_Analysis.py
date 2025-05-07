# %%
# HCUP Web-Scraped Hospital Data Analysis
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import numpy as np

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
# ## Business Question 1: How do readmission rates vary by condition and what trends are visible over time?

# %%
# SQL query with categorization and trend analysis using CASE statements
sql_query_1 = '''
WITH yearly_readmissions AS (
    SELECT 
        condition,
        year,
        AVG(readmission_rate) AS avg_readmission_rate,
        MIN(readmission_rate) AS min_readmission_rate,
        MAX(readmission_rate) AS max_readmission_rate,
        STDDEV(readmission_rate) AS stddev_readmission_rate,
        SUM(sample_size) AS total_sample_size
    FROM 
        raw.hcup_readmissions_raw
    GROUP BY 
        condition, year
),
readmissions_with_previous AS (
    SELECT 
        condition,
        year,
        avg_readmission_rate,
        min_readmission_rate,
        max_readmission_rate,
        stddev_readmission_rate,
        total_sample_size,
        LAG(avg_readmission_rate) OVER (PARTITION BY condition ORDER BY year) AS prev_year_rate
    FROM 
        yearly_readmissions
)
SELECT 
    condition,
    year,
    avg_readmission_rate,
    CASE 
        WHEN condition IN ('COPD', 'Pneumonia') THEN 'Respiratory'
        WHEN condition IN ('Heart Attack', 'Heart Failure') THEN 'Cardiac'
        WHEN condition = 'Hip/Knee Replacement' THEN 'Orthopedic'
        ELSE 'Other'
    END AS condition_category,
    CASE 
        WHEN avg_readmission_rate < 15 THEN 'Low'
        WHEN avg_readmission_rate < 20 THEN 'Medium'
        ELSE 'High'
    END AS readmission_severity,
    CASE 
        WHEN prev_year_rate IS NULL THEN NULL
        ELSE ROUND((avg_readmission_rate - prev_year_rate)::numeric, 2)
    END AS year_over_year_change,
    CASE 
        WHEN prev_year_rate IS NULL THEN 'First Year'
        WHEN (avg_readmission_rate - prev_year_rate) > 1 THEN 'Significant Increase'
        WHEN (avg_readmission_rate - prev_year_rate) > 0 THEN 'Slight Increase'
        WHEN (avg_readmission_rate - prev_year_rate) < -1 THEN 'Significant Decrease'
        WHEN (avg_readmission_rate - prev_year_rate) < 0 THEN 'Slight Decrease'
        ELSE 'No Change'
    END AS trend_direction,
    total_sample_size
FROM 
    readmissions_with_previous
ORDER BY
    condition_category,
    condition,
    year
'''

# Execute query and load results
readmission_by_condition = pd.read_sql(sql_query_1, engine)

# Display the results
print("Analysis of readmission rates by condition and year")
readmission_by_condition

# %%
# Visualize the trends of readmission rates by condition over time
plt.figure(figsize=(14, 8))

# Create line plots for each condition
for condition in readmission_by_condition['condition'].unique():
    condition_data = readmission_by_condition[readmission_by_condition['condition'] == condition]
    plt.plot(condition_data['year'], condition_data['avg_readmission_rate'], 
             marker='o', linewidth=2, label=condition)

plt.title('Readmission Rate Trends by Condition (2018-2022)', fontsize=14)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Readmission Rate (%)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Condition', fontsize=10)

# Add annotations for COVID-19 pandemic period
plt.axvspan('2020', '2021', alpha=0.2, color='red', label='COVID-19 Pandemic')
plt.text('2020.5', plt.ylim()[1]*0.9, 'COVID-19 Pandemic', 
         horizontalalignment='center', color='red', fontweight='bold')

plt.tight_layout()
plt.show()

# %%
# Create a stacked bar chart to visualize readmission severity distribution by condition
severity_pivot = pd.crosstab(
    readmission_by_condition['condition'], 
    readmission_by_condition['readmission_severity'],
    normalize='index'
) * 100

plt.figure(figsize=(12, 6))
severity_pivot.plot(kind='bar', stacked=True, colormap='RdYlGn_r')
plt.title('Distribution of Readmission Severity by Condition')
plt.xlabel('Condition')
plt.ylabel('Percentage')
plt.xticks(rotation=45)
plt.legend(title='Severity')
plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# There are significant variations in readmission rates across conditions, with distinct patterns emerging during the pandemic years (2020-2021). COPD shows the most dramatic spike during 2020-2021, potentially related to COVID-19 impacts. Most conditions display a pattern of increasing rates during the pandemic followed by decreases in 2022, suggesting recovery toward pre-pandemic levels.

# %% [markdown]
# ### Recommendation:
# Healthcare organizations should develop condition-specific readmission reduction strategies rather than one-size-fits-all approaches. For conditions like COPD and Heart Failure that showed the most volatility, additional post-discharge support may be warranted. The significant drops in 2022 should be studied to understand if they represent permanent improvements or temporary fluctuations.

# %% [markdown]
# ## Business Question 2: How do quality indicators correlate with readmission rates over time?

# %%
# SQL query for aggregate quality indicators and readmission rates by year
sql_query_2 = '''
WITH quality_by_year AS (
    SELECT 
        year,
        AVG(rate) AS avg_quality_rate,
        COUNT(DISTINCT indicator_id) AS indicator_count
    FROM 
        raw.hcup_quality_indicators_raw
    GROUP BY 
        year
),
readmissions_by_year AS (
    SELECT 
        year,
        AVG(readmission_rate) AS avg_readmission_rate,
        SUM(sample_size) AS total_sample_size
    FROM 
        raw.hcup_readmissions_raw
    GROUP BY 
        year
)
SELECT 
    q.year,
    q.avg_quality_rate,
    r.avg_readmission_rate,
    q.indicator_count,
    r.total_sample_size,
    CORR(q.avg_quality_rate, r.avg_readmission_rate) OVER () AS correlation
FROM 
    quality_by_year q
JOIN 
    readmissions_by_year r ON q.year = r.year
ORDER BY 
    q.year
'''

# Execute query and load results
quality_readmission_correlation = pd.read_sql(sql_query_2, engine)

# Display the results
print("Analysis of quality indicators and readmission rates by year")
quality_readmission_correlation

# %%
# Visualize the relationship between quality indicators and readmission rates
plt.figure(figsize=(12, 8))

# Create a line plot with dual y-axes
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot readmission rates on the first y-axis
color = 'tab:blue'
ax1.set_xlabel('Year')
ax1.set_ylabel('Average Readmission Rate (%)', color=color)
ax1.plot(quality_readmission_correlation['year'], 
         quality_readmission_correlation['avg_readmission_rate'], 
         color=color, marker='o', linewidth=2, label='Readmission Rate')
ax1.tick_params(axis='y', labelcolor=color)

# Create a second y-axis for quality rates
ax2 = ax1.twinx()
color = 'tab:red'
ax2.set_ylabel('Average Quality Rate', color=color)
ax2.plot(quality_readmission_correlation['year'], 
         quality_readmission_correlation['avg_quality_rate'], 
         color=color, marker='s', linewidth=2, label='Quality Rate')
ax2.tick_params(axis='y', labelcolor=color)

# Add correlation value to the title
correlation = quality_readmission_correlation['correlation'].iloc[0]
plt.title(f'Quality Indicators vs. Readmission Rates Over Time (r = {correlation:.2f})')

# Add a custom legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

plt.grid(True, linestyle='--', alpha=0.3)
plt.tight_layout()
plt.show()

# %% [markdown]
# ### Insight:
# There's a moderate negative correlation (-0.38) between quality indicator rates and readmission rates, suggesting that as quality improves (higher rates), readmissions tend to decrease. The relationship appears strongest during 2020-2022, when quality rates increased while readmission rates dropped, potentially indicating that quality improvement initiatives during the pandemic had measurable impacts on readmission outcomes.

# %% [markdown]
# ### Recommendation:
# Healthcare systems should continue investing in quality improvement programs as they appear to have a beneficial effect on reducing readmissions. Given the negative correlation, focusing on specific quality measures that showed the strongest improvement in 2022 could yield further readmission reductions. Organizations should also analyze which specific quality indicators have the strongest individual correlations with readmission reduction.

# %% [markdown]
# ## Business Question 3: How have readmission rates for different conditions changed over time, and which conditions show the most significant trends or volatility?

# %%
# SQL query to analyze readmission rate trends and volatility by condition
sql_query_3 = '''
WITH yearly_rates AS (
    SELECT 
        condition,
        year,
        readmission_rate,
        sample_size
    FROM 
        raw.hcup_readmissions_raw
),
trend_metrics AS (
    SELECT 
        condition,
        -- Calculate basic trend metrics
        MIN(readmission_rate) AS min_rate,
        MAX(readmission_rate) AS max_rate,
        MAX(readmission_rate) - MIN(readmission_rate) AS max_range,
        AVG(readmission_rate) AS avg_rate,
        STDDEV(readmission_rate) AS rate_stddev,
        
        -- Calculate coefficient of variation (standardized volatility)
        STDDEV(readmission_rate) / NULLIF(AVG(readmission_rate), 0) * 100 AS coefficient_of_variation
    FROM 
        yearly_rates
    GROUP BY 
        condition
),
yearly_changes AS (
    SELECT 
        condition,
        year,
        readmission_rate,
        LAG(readmission_rate) OVER (PARTITION BY condition ORDER BY year) AS prev_year_rate,
        readmission_rate - LAG(readmission_rate) OVER (PARTITION BY condition ORDER BY year) AS year_over_year_change,
        (readmission_rate - LAG(readmission_rate) OVER (PARTITION BY condition ORDER BY year)) / 
            NULLIF(LAG(readmission_rate) OVER (PARTITION BY condition ORDER BY year), 0) * 100 AS percent_change,
        sample_size
    FROM 
        yearly_rates
),
max_changes AS (
    SELECT 
        condition,
        MAX(ABS(year_over_year_change)) AS max_abs_change,
        MAX(ABS(percent_change)) AS max_abs_percent_change
    FROM 
        yearly_changes
    GROUP BY 
        condition
)
SELECT 
    y.condition,
    y.year,
    y.readmission_rate,
    y.prev_year_rate,
    y.year_over_year_change,
    y.percent_change,
    t.min_rate,
    t.max_rate,
    t.max_range,
    t.avg_rate,
    t.rate_stddev,
    t.coefficient_of_variation,
    m.max_abs_change,
    m.max_abs_percent_change,
    -- Rank conditions by volatility
    RANK() OVER (ORDER BY t.coefficient_of_variation DESC) AS volatility_rank,
    -- Rank conditions by max range
    RANK() OVER (ORDER BY t.max_range DESC) AS range_rank,
    -- Rank conditions by max absolute change
    RANK() OVER (ORDER BY m.max_abs_change DESC) AS abs_change_rank,
    y.sample_size
FROM 
    yearly_changes y
JOIN 
    trend_metrics t ON y.condition = t.condition
JOIN 
    max_changes m ON y.condition = m.condition
ORDER BY 
    t.coefficient_of_variation DESC, 
    y.condition, 
    y.year
'''

# Execute query and load results
readmission_trends = pd.read_sql(sql_query_3, engine)

# %%
# Display the results
print("Analysis of readmission rate trends and volatility by condition")
readmission_trends.head(10)










# %%
# Create visualizations for readmission rate trends and volatility
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# First, get the unique conditions and prepare for analysis
conditions = readmission_trends['condition'].unique()

# Create a line plot showing trends over time for each condition
plt.figure(figsize=(12, 6))
for condition in conditions:
    condition_data = readmission_trends[readmission_trends['condition'] == condition]
    plt.plot(condition_data['year'], condition_data['readmission_rate'], 
             marker='o', linewidth=2, label=condition)

plt.title('Readmission Rate Trends by Condition (2018-2022)', fontsize=14)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Readmission Rate (%)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Condition')
plt.tight_layout()
plt.show()

# %%
# Create a visualization of volatility metrics
volatility_metrics = readmission_trends.drop_duplicates('condition')[
    ['condition', 'coefficient_of_variation', 'max_range', 'max_abs_change', 
     'volatility_rank', 'range_rank', 'abs_change_rank']
].sort_values('volatility_rank')

plt.figure(figsize=(10, 6))
sns.barplot(x='condition', y='coefficient_of_variation', data=volatility_metrics)
plt.title('Volatility in Readmission Rates by Condition', fontsize=14)
plt.xlabel('Condition', fontsize=12)
plt.ylabel('Coefficient of Variation (%)', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

# %%
# Create a heatmap of year-over-year percent changes
# First, reshape the data for the heatmap
heatmap_data = readmission_trends.pivot_table(
    index='condition', 
    columns='year', 
    values='percent_change'
)

plt.figure(figsize=(12, 8))
sns.heatmap(heatmap_data, annot=True, cmap='RdBu_r', center=0, fmt='.1f')
plt.title('Year-over-Year Percent Change in Readmission Rates', fontsize=14)
plt.xlabel('Year', fontsize=12)
plt.ylabel('Condition', fontsize=12)
plt.tight_layout()
plt.show()

# %%
# Create a multi-metric comparison of conditions
plt.figure(figsize=(15, 8))

# Make sure max_abs_percent_change is included in volatility_metrics
volatility_metrics = readmission_trends.drop_duplicates('condition')[
    ['condition', 'coefficient_of_variation', 'max_range', 'max_abs_change', 
     'max_abs_percent_change', 'volatility_rank', 'range_rank', 'abs_change_rank']
].sort_values('volatility_rank')

# Plot the range (max - min)
plt.subplot(2, 2, 1)
sns.barplot(x='condition', y='max_range', data=volatility_metrics)
plt.title('Range of Readmission Rates')
plt.xlabel('')
plt.ylabel('Percentage Points')
plt.xticks(rotation=45)

# Plot the maximum absolute year-over-year change
plt.subplot(2, 2, 2)
sns.barplot(x='condition', y='max_abs_change', data=volatility_metrics)
plt.title('Maximum Year-over-Year Change')
plt.xlabel('')
plt.ylabel('Percentage Points')
plt.xticks(rotation=45)

# Plot the coefficient of variation
plt.subplot(2, 2, 3)
sns.barplot(x='condition', y='coefficient_of_variation', data=volatility_metrics)
plt.title('Coefficient of Variation')
plt.xlabel('Condition')
plt.ylabel('CV (%)')
plt.xticks(rotation=45)

# Plot the maximum absolute percent change
plt.subplot(2, 2, 4)
sns.barplot(x='condition', y='max_abs_percent_change', data=volatility_metrics)
plt.title('Maximum Percent Change')
plt.xlabel('Condition')
plt.ylabel('Percent')
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()


# %% [markdown]
# ### Insight:
# The analysis reveals significant volatility in readmission rates across conditions, with COPD and Heart Attack showing the highest variability (CV of 33.6% and 31.2% respectively). Most conditions exhibit pandemic-period disruptions during 2020-2021, followed by substantial normalization in 2022. COPD shows the most dramatic pattern with steady increases leading to a peak of 24.09% in 2021, then a sharp 53% decrease in 2022. Heart Attack displays the most dramatic year-over-year change with a 114% increase from 2021 to 2022, reversing a multi-year declining trend.

# %% [markdown]
# ### Recommendation:
# Healthcare organizations should implement condition-specific monitoring systems that can detect and respond to abrupt changes in readmission patterns. For highly volatile conditions like COPD and Heart Attack, develop intervention protocols that can rapidly scale up or down based on trending data. The significant volatility observed during the pandemic highlights the need for resilient care transition programs that can maintain effectiveness during healthcare disruptions. Additionally, the dramatic improvements seen in some conditions during certain periods should be studied to identify replicable best practices.