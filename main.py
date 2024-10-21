import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

def extract_from_csv(file_to_process):
    """Extract data from csv file"""
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    """Extract data from json file"""
    df = pd.read_json(file_to_process, lines=True)
    return df

def extract_from_xml(file_to_process):
    """Extract data from xml file"""
    # create empty dataframe
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
        # each newly opened file will start counting index from 0, hence ignore index
    return df
    
def extract():
    """
    Extract all data from across different file types
    Return data in single DataFrame
    """
    # create an empty dataframe that we will fill up with extracted data
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])
    
    # process all csv files
    for csvfile in glob.glob("data/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("data/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob("data/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

def transform(data):
    """
    Convert inches to metres and round to two decimal places;
    1 inch is 0.0254 metres
    Convert pounds to kilogrammes and round to two decimal places;
    1 pound is 0.45359237 kg
    """
    data["height"] = round(data.height * 0.0254, 2)
    data["weight"] = round(data.weight * 0.45359237, 2)

    return data

def load_data(target_file, transformed_data):
    """Take the transformed data and load it into the target file"""
    transformed_data.to_csv(target_file)

def log_progress(message):
    """Append messages to a logfile as the ETL process runs"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp},{message}\n")
    
# main
log_file = "log_file.txt" 
target_file = "transformed_data.csv"

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract()
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 

