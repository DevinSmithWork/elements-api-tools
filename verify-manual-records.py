# This script parses a CSV file containing manual record [data source proprietary id]s from the input/ directory, and verifies them with the same comment.

import creds

from csv import DictReader
import pyodbc
from pprint import pprint
import xml.etree.ElementTree as ET
import requests
from time import sleep

# load the appropriate creds
api_creds = creds.api_creds_qa


# ---------------------------------
def load_manual_record_ids():

    with open("input/input-test.csv", encoding='utf-8-sig') as file:
        reader = DictReader(file)
        ud = [row for row in reader]
        return ud


# ---------------------------------
# For verification, we only need one body XML for each item.
# This is returned once, then assigned to every item in the list.
def create_verification_xml():

    root = ET.Element('update-record', xmlns="http://www.symplectic.co.uk/publications/api")

    v_status = ET.SubElement(root, "verification-status")
    ET.SubElement(v_status, "text").text = "verified"

    v_comment = ET.SubElement(root, "verification-comment")
    ET.SubElement(v_comment, "text").text = "DS: Verified via API, pub-date mismatches 2024-01-29"

    xml_body = ET.tostring(root)
    return xml_body


# ---------------------------------
def update_records_via_api(ud):

    for item in ud:

        # Append the user record URL to the endpoint
        req_url = api_creds['endpoint'] + "publication/records/manual/" + item['manual-record-id']

        # Content type header is required when sending XML to Elements' API.
        headers = {'Content-Type': 'text/xml'}

        # Send the http request
        response = requests.patch(req_url,
                                  headers=headers,
                                  data=item['body_xml'],
                                  auth=(api_creds['username'], api_creds['password']))

        # Report on updates
        if response.status_code == 200:
            print("\nSuccessful update:", item['manual-record-id'])

        else:
            print("\nNon-200 status code received:")
            pprint(response.status_code)
            pprint(response.headers['content-type'])
            pprint(response.text)

    sleep(2)


# ------------------------------------------------
if __name__ == '__main__':

    update_dicts = load_manual_record_ids()

    body_xml = create_verification_xml()

    for item in update_dicts:
        item['body_xml'] = body_xml

    update_records_via_api(update_dicts)
