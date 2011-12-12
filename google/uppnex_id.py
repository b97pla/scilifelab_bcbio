#!/usr/bin/env python
"""Module for extracting a project's UppNex id from a spreadsheet on Google Docs"""

import bcbio.google
import bcbio.google.spreadsheet
import os
from bcbio.pipeline import log

PROJECT_DATA_COLUMNS = (
    "ID",
    "Project name",
    "Queue date",
    "No of samples",
    "Lanes / Plates",
    "minimal M read pairs/sample (passed filter)",
    "Customer reference",
    "Application",
    "No of samples finished (All sequencing finished)",
    "Uppnex ID"
)
PROJECT_NAME_COLUMN = (
    "Project name"
)

def get_project_data(project_name,config,columns=PROJECT_DATA_COLUMNS,name_column=PROJECT_NAME_COLUMN):
    """Fetch project related data from the Google spreadsheets"""
    
    # Initialize the data array
    data = dict(zip(columns,["N/A"]*len(columns)))
    
    encoded_credentials = bcbio.google.get_credentials(config)
    if encoded_credentials is None:
        log.warn("The Google Docs credentials could not be found.")
        return data
    
    # Get the name of the spreadsheet where uppnex ids can be found
    gdocs_config = config.get("gdocs_upload",{})
    ssheet_title = gdocs_config.get("projects_spreadsheet",None)
    wsheet_title = gdocs_config.get("projects_worksheet",None)
    if not ssheet_title or not wsheet_title:
        log.warn("The names of the projects spreadsheet and worksheet on Google Docs could not be found.")
        return data
    
    # Connect to the spread- and worksheet
    client = bcbio.google.spreadsheet.get_client(encoded_credentials)
    ssheet = bcbio.google.spreadsheet.get_spreadsheet(client,ssheet_title)
    if not ssheet:
        log.warn("Could not connect to %s on Google Docs." % ssheet_title)
        return data
    
    # We allow multiple, comma-separated worksheets to be searched
    for wtitle in wsheet_title.split(','):
        wsheet = bcbio.google.spreadsheet.get_worksheet(client,ssheet,wtitle.strip())
        if not wsheet:
            log.warn("Could not locate %s in %s." % (wsheet_title,ssheet_title))
            continue

        # Get the rows for the project
        rows = bcbio.google.spreadsheet.get_rows_with_constraint(client,ssheet,wsheet,{name_column: project_name})
        print rows
        if len(rows) == 0:
            continue
        
        # Get the header row for the worksheet
        header = bcbio.google.spreadsheet.get_header(client,ssheet,wsheet)
        # Match the data columns to the header
        for i, column in enumerate(columns):
            index = bcbio.google.spreadsheet.get_column_index(client,ssheet,wsheet,column)
            if index > 0:
                data[column] = ", ".join([row[index-1] for row in rows])
    return data    

def get_project_uppnex_id(project_name,config):
    """Attempt to fetch the Uppnex id associated with a project"""
    
    UPPNEXID_COLUMN = 'Uppnex ID'
    id = get_project_data(project_name,config,(UPPNEXID_COLUMN))
    return id[0]
