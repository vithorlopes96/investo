import json


class EventHandler:
    """Class representing a person"""
    
    def __init__(self,event_file_path) -> None:
        self.json_event = None


    def fetch_event(self, file_path: __path__):
        """
        Handles an event that contains a JSON with the API URL and tasks (parameters for the API calls).
        """
        # Parse the JSON event
        event_data = json.loads(file_path)

        # Extract the URL and tasks from the event
        return event_data

    def get_required_api_params(self, event_data):
        
        required_params = json.load("path to dict") 
        api_name = event_data.get('api_name')
        
        # Get the required parameters for the specified API
        if api_name not in required_params:
            raise ValueError(f"API '{api_name}' is not defined in the parameters.")
        
        required_params = required_params[api_name]['required_params']
        
        # Extract parameters from the event
        return required_params

    def validate_event(self, event_data: object, required_params: list):    # Ensure that both the URL and tasks are provided
        try:
            params = [event_data.get(param) for param in required_params] 

            if set(params) == set(required_params):
                raise ValueError("The required params dont macht with event params.")
    
            return True
        except ValueError:
            return False
    
    def main(self, file_path):
        
        event = self.fetch_event(file_path)
        required_params = self.get_required_api_params(event)
        validate_event = self.validate_event(event, required_params)
        if validate_event:
            return event
        else:
            raise ValueError('The Event is not valid, please check the event file')
        



