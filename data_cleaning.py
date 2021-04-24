#Import Libraries
import numpy as np
import pandas as pd

#Read the datasets
#Note these datasets should be saved in the same dir as the notebook
minimum_wages_table = pd.read_csv (r'minimum_wages_data.csv')
top_things = pd.read_csv(r'top_things.csv')
median_income = pd.read_csv(r'median_income_modified.csv')
gini_table = pd.read_csv (r'Frank_Gini_2018.csv')
unemployment_state = pd.read_csv(r'state_unemployment.csv')
gdp_per_state = pd.read_csv(r'gpd_per_state.csv')
senators = pd.read_csv(r'1976-2020-senate.csv')

#Cleaning the population data 
state_pop_from_2010 = pd.read_csv("state_population_from_2010_modified.csv")
state_pop_until_2010 = pd.read_csv("state_pop.csv")
state_pop_from_2010 = state_pop_from_2010.rename(columns={"Geographic Area": "State"})
state_pop_until_2010 = state_pop_until_2010.rename(columns={"Area Name": "State"})
state_pop_from_2010['State'] = state_pop_from_2010['State'].str[1:]
state_population = pd.merge(state_pop_until_2010, state_pop_from_2010, on=["State"])


#Cleaning senators dataset
senators["state"] = senators["state"].str.capitalize()
def count_democratic_percentage(state, year):
    total_seats = len(senators.loc[(senators['state'] == state) & (senators['year'] == year)]["party_simplified"])
    seats = senators.loc[(senators['state'] == state) & (senators['year'] == year)]["party_simplified"]
    democrat = 0
    if total_seats == 0:
        return np.nan
    
    for seat in seats: 
        if seat == "DEMOCRAT":
            democrat += 1
    return democrat/total_seats

#Read the dataset heads
median_income.head()
minimum_wages_table.head()

#Fix the column names for median_income dataset
change_col_names = {}
for col in median_income.columns: 
    change_col_names[col] = col.split(" ")[0]
median_income = median_income.rename(columns=change_col_names)

#Fix the column names for gdp_per_state dataset
change_col_names = {}
for col in gdp_per_state.columns[4:]: 
    change_col_names[col] = col.split("-")[1]
gdp_per_state = gdp_per_state.rename(columns=change_col_names)

#Combine the datasets
combined_dataset = pd.merge(minimum_wages_table, gini_table, on=["Year", "State"])
combined_dataset = pd.merge(combined_dataset, top_things, on=["Year", "State"])

#Flip the median_income table to match the minimum wages table, and drop the missing data
combined_dataset["median_income"] = np.nan
for indx, state in enumerate (median_income.State.unique()):
    for year in median_income.columns[1:]:
        if state in combined_dataset.State.unique():
            med_income = median_income.iloc[indx][year]
            row = combined_dataset.loc[(combined_dataset['State'] == state) & (combined_dataset['Year'] == int(year))].index
            if type(med_income) == str:
                combined_dataset.at[row, "median_income"] = int(med_income.replace(',', ''))
        else: 
            continue

#Flip the median_income table to match the unempolyment_state table, and drop the missing data
combined_dataset["unempolyment_state"] = np.nan
for indx, state in enumerate (unemployment_state.Area.unique()):
    for year in unemployment_state.columns[2:]:
        if state in combined_dataset.State.unique():
            unempolyment = unemployment_state.iloc[indx][year]
            row = combined_dataset.loc[(combined_dataset['State'] == state) & (combined_dataset['Year'] == int(year))].index
            combined_dataset.at[row, "unempolyment_state"] = int(unempolyment)
        else: 
            continue


#Flip the gdp_per_state table to match the unempolyment_state table, and drop the missing data
combined_dataset["cur_dol_gdp_change"] = np.nan
gdp_per_state = gdp_per_state[gdp_per_state.Description == "Current-dollar GDP (millions of current dollars)"]
for indx, state in enumerate (gdp_per_state.GeoName.unique()):
    for year in gdp_per_state.columns[4:]:
        if state in combined_dataset.State.unique():
            gdp_change = gdp_per_state.iloc[indx][year]
            row = combined_dataset.loc[(combined_dataset['State'] == state) & (combined_dataset['Year'] == int(year))].index
            if gdp_change != "(NA)":
                combined_dataset.at[row, "cur_dol_gdp_change"] = float(gdp_change)
        else: 
            continue
            
#Flip the state population table to match the unempolyment_state table, and drop the missing data
combined_dataset["state_population"] = np.nan
for indx, state in enumerate (state_population.State.unique()):
    for year in state_population.columns[1:]:
        if state in combined_dataset.State.unique():
            population = state_population.iloc[indx][year]
            row = combined_dataset.loc[(combined_dataset['State'] == state) & (combined_dataset['Year'] == int(year))].index
            combined_dataset.at[row, "state_population"] = float(population.replace(',', ''))
        else: 
            continue

#Cleaning the senators dataset
combined_dataset["democratic_percentage"] = np.nan
for indx, state in enumerate (senators.state.unique()):
    for year in senators.year.unique():
        if state in combined_dataset.State.unique() and year in combined_dataset.Year.unique():
            row = combined_dataset.loc[(combined_dataset['State'] == state) & (combined_dataset['Year'] == int(year))].index
            combined_dataset.at[row, "democratic_percentage"] = count_democratic_percentage(state, year)
        else: 
            continue
            
#Drop the missing data
combined_dataset = combined_dataset[combined_dataset['median_income'].notna()]
combined_dataset = combined_dataset[combined_dataset['cur_dol_gdp_change'].notna()]
combined_dataset = combined_dataset[combined_dataset['state_population'].notna()]
combined_dataset = combined_dataset[combined_dataset['democratic_percentage'].notna()]

combined_dataset
#Save the dataset as CSV
combined_dataset.to_csv('cleaned_dataset.csv', index=False)