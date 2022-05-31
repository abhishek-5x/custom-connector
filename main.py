import json
import requests
from typing import Dict, List, Any
from datetime import datetime
import logging
import sys
import datetime
import functions_framework


#Global variabes for API components
base_url = "http://v3.football.api-sports.io"
headers = {'x-rapidapi-key': '7efb683cc5bff3cbfe849defa026ec7e'}
params = {"countries":{},"leagues":{'code':'GB'},"venues":{'country':'England'}}


# List of endpoints we need to hit for the API
ENDPOINTS = [
    "countries",
    "leagues",
    "venues"
]

#List of schemas and primary keys in every schema/table. We need to provide this for Fivetran to know which rows are new ones basis primary key 
SCHEMA = {
    "countries": {"primary_key": ["code"]},
    "leagues": {"primary_key":["id"]},
    "venues": {"primary_key":["id"]}
}


# A dictionary which will be used filter data from the API Response
endpoint_segment = {"countries":"countries","leagues":"leagues","venues":"venues"}



#Function for logging
logging.basicConfig(
    format='{"time": %(asctime)s, "name": %(name)s, "level": %(levelname)s", "message": %(message)s}',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)



# This function is where the actual API call is done
# This is called by the lambda handler function and the json response is returned
def get_data(endpoint,params):
    url = f"{base_url}/{endpoint['object']}"
    try:
        response = requests.get(url,headers=headers,params=params[endpoint['object']]).json()
    except KeyError as e:
        print(f"Wrong format url {url}", e)
    return response




# Default value for initializing the last_timestamp while initializing state
DEFAULT_START_TIME = "2000-01-01T00:00:00.000Z"



@functions_framework.http
def lambda_handler(request, context):

    # add custom connector code

    config = request.get("secrets", {})
    state = request.get("state", {})

    # Set Parameters - Params Dictionary
    api_key = config["api_key"]


    # Set/Get State
    """
    If all the objects are has_more = False, set them all to true
    """
    if state == {}:
        state = set_state()

    all_false = True
    for item in ENDPOINTS:
        if state[item]["has_more"] == True:
            all_false = False

    if all_false == True:
        for item in ENDPOINTS:
            state[item]["has_more"] = True

    current_endpoint = [
        state[endpoint] for endpoint in state if state[endpoint]["has_more"] == True
    ][0]




    # Section to Pull Data

    # Calling the get_data function for the current endpoint which will return a json response
    records = get_data(current_endpoint,params)

    """ 
    Format the json resonse to get only the required data
    For example, in case of leads endpoint, the response looks like: {"leads": {"account_id":1,"id":1},{"account_id":2,"id":2}}
    In the line below, using the code on the right of the "=", we are referring to "records[leads]" which will give us the following:
    {"account_id":1,"id":1},{"account_id":2,"id":2} which is exactly what we need
    And, we store this into formatted_records
    """
    formatted_records = []

    for record in records['response']:
        frecord = {'updated_at': str(datetime.datetime.now()),'data':record}
        formatted_records.append(frecord)
        

    # Inserts is a part of the response returned to Fivetran
    inserts = {current_endpoint["object"]: formatted_records}

    # Set New State
    """
    Set the timestamp from the last recieved record
    """
    if len(formatted_records)>0:
        state[current_endpoint["object"]]["last_timestamp"] = formatted_records[-1]["updated_at"]

    # Check for More
    """
    Do we need to paginate again on the current record?
    Ready to go to the next object?
    All objects completed?


    if len(formatted_records) < page_size:
        state[current_endpoint["object"]]["has_more"] = False
    else:
        state[current_endpoint["object"]]["has_more"] = True
        # We will paginate (Increment the page value) and check if there are any records left on the next page
        page[current_endpoint["object"]]+=1
    """    

    has_more = False
    
    state[current_endpoint["object"]]["has_more"] = False
    for endpoint in ENDPOINTS:
        if state[endpoint]["has_more"] == True:
            has_more = True


    return response(
        state=state,
        schema=SCHEMA,
        inserts=inserts,
        deletes={},
        hasMore=has_more,
    )



def set_state() -> Dict:
    state = {}
    for item in ENDPOINTS:
        state[item] = {
            "object": item,
            "has_more": True,
            "last_timestamp": DEFAULT_START_TIME,
        }
    return state



def response(
    state: Dict[str, Any],
    schema: Dict[Any, Any],
    inserts: Dict[Any, Any] = {},
    deletes: Dict[Any, Any] = {},
    hasMore: bool = False,
):
    """Creates the response JSON object that will be processed by Fivetran."""
    return {
        "state": state,
        "schema": schema,
        "insert": inserts,
        "delete": deletes,
        "hasMore": hasMore,
    }



# Test API Call

request = {"secrets": {"api_key":"7efb683cc5bff3cbfe849defa026ec7e"}, "state": {}}

i = 1


while True:
    print(f"\n\n Iteration number: {i}")
    new_state = lambda_handler(request , '')
    print(new_state)
    if(new_state['hasMore'] == False):
        break
    request["state"] = new_state['state']
    i+=1

