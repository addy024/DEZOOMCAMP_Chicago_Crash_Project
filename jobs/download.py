import pandas as pd
import requests

def fetch_data_with_offset(api_url, offset):
    try:
        params = {'$offset': offset}
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        return response.json()  # Parse JSON response
    except requests.exceptions.RequestException as e:
        print("Error fetching data:", e)
        return None

# Example usage
api_url = "https://data.cityofchicago.org/resource/85ca-t3if.json"

# Define the initial offset and increment value
offset = 0
increment = 1000

# Fetch data in increments of 1000 rows
all_data = []
while True:
    data = fetch_data_with_offset(api_url, offset)
    if not data:
        print("Failed to fetch data from the API.")
        break
    elif len(data) == 0:
        print("Reached the end of the dataset.")
        break
    else:
        print("Fetched", len(data), "rows starting from offset", offset)
        all_data.extend(data)
        offset += increment
    if offset > 10000:
        break

# Convert fetched data to a DataFrame
df = pd.DataFrame(all_data)

# Save DataFrame to a CSV file
df.to_csv("crash.csv", index=False)

print("Data saved to crash.csv")
