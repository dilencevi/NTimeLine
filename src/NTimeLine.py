import sqlite3
import pandas as pd
import sys

if len(sys.argv) != 3:
    print ("""\
            This script connects to the SQLite database, performs the query to get the timeline 
            of events and returns a pandas DataFrame containing the results. The results are
            printed out into a file.

            Usage: NTimeLine <path_to_the_SQLite_file> <path_to_and_name_of_output_file>
            """)
    sys.exit(0)

# Get a path to a SQLite file
db_path = sys.argv[1]

# Get a path and file name for an output
output_file = sys.argv[2]

# Function to perform query and return DataFrame
def get_event_timeline(sqlite_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_file)

    # SQL query to retrieve the timeline of events with timestamp, event string, module string, and stackDepth
    query = """
        SELECT
            e.start as timestamp,
            s2.value as module_string,
            s1.value as event_string,
            c.stackDepth
        FROM
            COMPOSITE_EVENTS e
            JOIN SAMPLING_CALLCHAINS c ON e.id = c.id
            JOIN StringIds s1 ON c.symbol = s1.id
            JOIN StringIds s2 ON c.module = s2.id
        ORDER BY
            e.start, c.stackDepth
    """

    # Execute the query and store the result in a DataFrame
    df = pd.read_sql(query, conn)

    # Write the DataFrame to a CSV file
    df.to_csv(output_file, index=False)

    # Close the connection to the database
    conn.close()

    return df

# Get the timeline DataFrame
timeline_df = get_event_timeline(db_path)

# Print the first few rows of the DataFrame
print(timeline_df.head())
