# Business Problem

Problem: Customer conversion across the ecommerce site has been identified as a potential area of improvement.

Analysis Objectives:
 1. Assess customer conversion through key steps of the funnel
 2. Analyze and identify differences in conversion across customer segments and cohorts
 3. Investigate trends in user conversion over time

Business Objective: identify steps in the ecommerce funnel for the product teams to focus on to increase conversion, and highlight any key user segments to focus on for potential product optimization or marketing campaigns.

# Analysis Plan (To be deleted later)

## Planned Analyses
High-level understanding of customer conversion:
 1. Aggregate conversion rate across all customers and time
 2. Conversion rates over time (daily trends) + time based cohort analysis
 3. Conversion rates for geographic segments
 4. Conversion rates for device segments
 5. Session conversion. Key questions:
    a. How often do customers convert in the first session?
    b. How quickly do customers convert in a given session?
    c. How long does each step typically take?
    d. Does the length of a session affect conversion rates?

## Next Steps
- Expand unit tests to other classes and methods
- Expand segmentation analysis to use clustering
- Test data quality of junk sessions (i.e. that lasted very short time, likely less than 5 seconds)

# Key Findings

# Technical Approach

## Files for real-time automation and reproducability
- data_loader.py --> file that loads using SQL queries. SQL queries manage pre-proessing
- data_processor.py --> script for creating the data processor class that implements deeper analyses on demand, like customer segmentation and cohorts
- visualization_engine.py --> script with project-specific class for custom data visualizations
- segmentation.py --> script to implement class for customer segmentation analyses
- tests.py --> unit tests to ensure classes are working properly

## Markdown files for inital presentation to stakeholders
- Funnel_Analysis_Results.ipynb --> Markdown for using the above classes to generate shareable report with stakeholders

# Repository Structure

|-notebooks/
|----dataset_exploration.ipynb
|----workshop.ipynb
|----Funnel_Analysis_Results.ipynb
|-src/
|----data_loader.py
|----data_processor.py
|----visualization_engine.py
|----tests.py

# How to Reproduce

Install all required packages in the requirements.txt file.
Run all code chunks in the "final_output.ipynb" file.

If you encounter problems with with the final_output.ipynb file, change your settings for the default jupyter directory in VS code settings.