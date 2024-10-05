import json
import csv
import os


class Writer:
    """Base writer class that defines the writing interface."""

    def write(self, data):
        raise NotImplementedError("Subclasses must implement this method")


class CSVWriter(Writer):
    """Handles writing data to a CSV file."""
    
    def __init__(self, file_path, fieldnames):
        self.file_path = file_path
        self.fieldnames = fieldnames
        
    def write(self, data):
        """Writes data to a CSV file."""
        file_exists = os.path.isfile(self.file_path)
        
        with open(self.file_path, mode='a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=self.fieldnames)
            
            if not file_exists:
                writer.writeheader()  # Write header only if the file does not exist
            
            writer.writerows(data)
        print(f"Data written to {self.file_path}")


class JSONWriter(Writer):
    """Handles writing data to a JSON file."""
    
    def __init__(self, file_path):
        self.file_path = file_path
        
    def write(self, data):
        """Writes data to a JSON file."""
        with open(self.file_path, mode='w') as json_file:
            json.dump(data, json_file, indent
