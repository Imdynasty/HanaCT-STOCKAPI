import csv
import json

# Read the CSV file with utf-8-sig to handle BOM
csv_file_path = "/Users/smin/Downloads/코스피지수 과거 데이터 (1).csv"
json_file_path = "/Users/smin/Downloads/nasdaq_data.json"

data = []

# Use 'utf-8-sig' encoding to handle BOM
with open(csv_file_path, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Create a dictionary for each row and append it to the data list
        data.append({
            "date": row["날짜"].strip(),
            "closing_price": row["종가"].strip(),
            "variation": row["변동 %"].strip()
        })

# Write the data to a JSON file
with open(json_file_path, 'w', encoding='utf-8') as jsonfile:
    json.dump(data, jsonfile, ensure_ascii=False, indent=4)

print(f"Data has been successfully converted to JSON and saved to {json_file_path}")
