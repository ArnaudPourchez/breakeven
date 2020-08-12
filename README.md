# Breakeven
Repository containing examples of the scripts used to get the data used in the "Break-even monthly cohort by media" google sheet

# Explanation
Here, there are three scripts thast works together. Each have a specific functionality:
- quick_date_range: creates a list of the desired dates for our monthly cohorts;
- campaign_list: encapsulated SQL query that pulled a list of desired campaign_id and name to return them as a DataFrame.
- monthly_cohort_per_campaign: the main scripts that uses the previous scripts to identify all the users who signed up at a specific month an then track their spending until the current date.

# About SQL
You will realize that the queries have quite some fancy joins conditions. This is due to the our MySQL database version that does not support the "WITH () AS" conditions.