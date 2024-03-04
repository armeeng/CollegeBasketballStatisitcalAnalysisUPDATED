from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np
from datetime import datetime

startTime = datetime.now()

pd.set_option('display.max_rows', None) # So you can view the whole table
pd.set_option('display.max_columns', None) # So you can view the whole table

# MySQL Database Connection Details
db_username = "root"
db_password = "Jerome43016"
db_host = "localhost"
db_port = "3306"
db_name = "college_basketball_statistics_random"

# SQLAlchemy Connection String
connection_str = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create SQLAlchemy Engine
engine = create_engine(connection_str)

game_results_query = "SELECT * FROM game_results"
game_results_table_data = pd.read_sql(game_results_query, engine)

def create_random_table(lowerBound, upperBound, height, width):
    random_table = pd.DataFrame(np.random.randint(lowerBound, upperBound, size=(height, width)))
    return random_table

def multiply_table(data_table, random_table):
    # Get the number of columns in each table
    num_columns_data = len(data_table.columns)
    num_columns_random = len(random_table.columns)

    # Initialize a new DataFrame to store the multiplied values
    multiplied_table = pd.DataFrame()

    # Perform element-wise multiplication for each column in the random table
    for i in range(num_columns_random):
        if i < num_columns_data:  # Check if data_table has enough columns
            multiplied_table[i] = data_table.iloc[:, i] * random_table.iloc[:, i]
        else:
            # If data_table has fewer columns, break the loop
            break

    return multiplied_table

def sum_data_table(data_table):
    # Select numeric values only
    numeric_values = data_table.select_dtypes(include=['number']).to_numpy(na_value=0)

    # Sum all numeric values in the DataFrame
    total_sum = numeric_values.sum()

    return total_sum

lower_random_bound = -2
upper_random_bound = 2

random_table1 = create_random_table(lower_random_bound, upper_random_bound, 110, 6)
random_table2 = create_random_table(lower_random_bound, upper_random_bound, 7, 4)
random_table3 = create_random_table(lower_random_bound, upper_random_bound, 17, 1)

correct = 0
incorrect = 0

for index, row in game_results_table_data.iterrows():
    data_difference_sum = 0

    Team1 = row.iloc[0]
    Team2 = row.iloc[2]
    date_of_game = row.iloc[4]
    winner = row.iloc[6]

    table1_query = f"SELECT * FROM `z{Team1}_{Team2}_{date_of_game}_table1`"
    table1_data = pd.read_sql(table1_query, engine)

    table2_query = f"SELECT * FROM `z{Team1}_{Team2}_{date_of_game}_table2`"
    table2_data = pd.read_sql(table2_query, engine)

    table3_query = f"SELECT * FROM `z{Team1}_{Team2}_{date_of_game}_table3`"
    table3_data = pd.read_sql(table3_query, engine)

    random_multiplied_table1 = multiply_table(table1_data, random_table1)
    random_multiplied_table2 = multiply_table(table2_data, random_table2)
    random_multiplied_table3 = multiply_table(table3_data, random_table3)

    data_difference_sum = sum_data_table(random_multiplied_table1) + sum_data_table(random_multiplied_table2) + sum_data_table(random_multiplied_table3)

    if data_difference_sum <= 0:
        predicted_winner = Team2
    else:
        predicted_winner = Team1

    if predicted_winner == winner:
        correct = correct + 1
    else:
        incorrect = incorrect + 1


accuracy = correct / (correct+incorrect)
print(random_table1)
print(random_table2)
print(random_table3)
print(accuracy)

endTime = datetime.now()
print(endTime - startTime)






