# Business Problem

Problem: Customer conversion across the ecommerce site has been identified as a potential area of improvement.

Analysis Objectives:
 1. Assess customer conversion through key steps of the funnel
 2. Analyze and identify differences in conversion across customer segments and cohorts
 3. Investigate trends in user conversion over time

Business Objective: identify steps in the ecommerce funnel for the product teams to focus on to increase conversion, and highlight any key user segments to focus on for potential product optimization or marketing campaigns.

# Key Findings

- Over half of customers drop off between adding to cart and purchase. This suggests possible issues with the checkout process and is worth further investigation
- A large spike in traffic in the December timeframe suggests a sales strategy. For product specifically, it appears conversion rates in key steps dropping during this time. It's likely the new traffic was net-new users of the sites, and the drops in user conversion may suggest new users struggle with navigating the site
- There is a data anomaly for the first few weeks of the collection period where total page views is less than total purchases. This warrants follow up with the team that implemented the tracking for more background on the collection process

## Plans for Future Extensions
- Expand unit tests to other classes and methods
- Session Conversion Rates

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