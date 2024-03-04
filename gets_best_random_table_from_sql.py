from sqlalchemy import create_engine
import pandas as pd

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

# Load the best accuracy from the database
best_50_df = pd.read_sql('best_50_accuracies', engine, index_col='Accuracy')
best_accuracy_row = best_50_df.iloc[0]  # Retrieve the row with the highest accuracy

# Extract random tables from the best accuracy row
best_random_table1 = best_accuracy_row['Random Table 1']
best_random_table2 = best_accuracy_row['Random Table 2']
best_random_table3 = best_accuracy_row['Random Table 3']

# Now you have the best random tables stored in variables
print("Best Random Table 1:")
print(best_random_table1)
print("\nBest Random Table 2:")
print(best_random_table2)
print("\nBest Random Table 3:")
print(best_random_table3)