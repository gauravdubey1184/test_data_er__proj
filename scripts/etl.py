import csv

input_file = "data/input.csv"
output_file = "output/output.csv"

filtered_data = []

with open(input_file, mode='r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        if int(row['age']) > 18:
            filtered_data.append(row)

with open(output_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['name','age','salary'])
    writer.writeheader()
    writer.writerows(filtered_data)

print("ETL Process Completed")