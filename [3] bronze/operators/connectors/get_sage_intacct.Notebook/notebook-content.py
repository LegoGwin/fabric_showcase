# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

import requests
import time
import xml.etree.ElementTree as xml_parser

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retry_limit = 4

kv_url = 'https://bmgkeyvault01.vault.azure.net/'

sender_id = mssparkutils.credentials.getSecret(kv_url, 'sage-sender-id')
sender_pw = mssparkutils.credentials.getSecret(kv_url, 'sage-sender-pw')
user_id = mssparkutils.credentials.getSecret(kv_url, 'sage-user-id')
company_id = sender_id
user_pw = mssparkutils.credentials.getSecret(kv_url, 'sage-user-pw')
session_id = None

request_url = 'https://api.intacct.com/ia/xml/xmlgw.phtml'

request_headers = {'Content-Type': 'application/xml'}

request_template = \
    """<?xml version="1.0" encoding="UTF-8"?>
        <request>
            <control>
                <senderid>{sender_id}</senderid>
                <password>{sender_pw}</password>
                <controlid></controlid>
                <uniqueid>false</uniqueid>
                <dtdversion>3.0</dtdversion>
                <includewhitespace>false</includewhitespace>
            </control>
            <operation>
                <authentication>
                    {auth_body}
                </authentication>
                <content>
                    <function controlid="">
                        {function_body}
                    </function>
                </content>
            </operation>
        </request>
    """

auth1_template = \
    """
        <login>
            <userid>{user_id}</userid>
            <companyid>{company_id}</companyid>
            <password>{user_pw}</password>
        </login>
    """

auth2_template = "<sessionid>{session_id}</sessionid>"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def request_session_id():
    auth_body = auth1_template.format(user_id = user_id, company_id = company_id, user_pw = user_pw)
    function_body = "<getAPISession />"
    request_body = request_template.format(sender_id = sender_id, sender_pw = sender_pw, auth_body = auth_body, function_body = function_body)
    
    try:
        response = requests.post(request_url, headers = request_headers, data = request_body)
        return response
    except:
        return None

def extract_session_id(response):
    try:
        response_xml = xml_parser.fromstring(response.text)
        session_id_element = response_xml.find('.//sessionid')
        session_id = session_id_element.text if session_id_element is not None else None
        return session_id
    except:
        return None

def get_session_id():
    response = request_session_id()
    session_id = extract_session_id(response)

    return session_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def set_session_id(retry_counter = 0):
    global session_id

    while retry_counter < retry_limit:
        session_id = get_session_id()

        if session_id is not None:
            return True
        
        time.sleep(2 ** retry_counter)
        retry_counter += 1

    return False

set_session_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def response_error(response):
    if response.status_code != 200:
        return 'Status code is not 200.'

    content_type = response.headers.get('Content-Type', '')
    xml_content = 'xml' in content_type.lower()

    if xml_content:
        try:
            response_xml = xml_parser.fromstring(response.text)

            error_element = response_xml.find('.//errorno')
            if error_element is not None:
                error_no = error_element.text
                if error_no == 'XL03000006':
                    return 'Session expired.'
                else:
                    return 'Response has an error.'

            status_element = response_xml.find('.//status')
            if status_element is None:
                return 'Response missing status.'

            if status_element.text == 'failure':
                return 'Response failed status.'
        except:
            return 'Response xml is unexpected.'

    return None

def request_object(function_body, retry_counter = 0):
    auth_body = auth2_template.format(session_id = session_id)
    request_body = request_template.format(sender_id = sender_id, sender_pw = sender_pw, auth_body = auth_body, function_body = function_body)

    while retry_counter < retry_limit:
        response = requests.post(request_url, headers = request_headers, data = request_body)
        
        error_text = response_error(response)
        if error_text is None:
            return response

        if "session expired" in error_text.lower():
            set_session_id()
            auth_body = auth2_template.format(session_id = session_id)
            request_body = request_template.format(sender_id = sender_id, sender_pw = sender_pw, auth_body = auth_body, function_body = function_body)
        
        time.sleep(2 ** retry_counter + 5)
        retry_counter += 1

    raise Exception(f"Request failed after {retry_limit} retries: {error_text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def request_object_fields(source_object):
    function_template = "<lookup><object>{source_object}</object></lookup>"
    function_body = function_template.format(source_object = source_object)       

    response = request_object(function_body)

    return response

def extract_object_fields(response):
    response_text = response.text
    response_xml = xml_parser.fromstring(response_text)

    field_elements = response_xml.findall(".//Field")
    field_list = ""
    for field_element in field_elements:
        id_element = field_element.find("ID")
        if id_element is not None:
            field_list = field_list + f"<field>{id_element.text}</field>\n"
    
    return field_list

def get_object_fields(source_object):
    response = request_object_fields(source_object)
    fields = extract_object_fields(response)

    return fields

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def request_object_data(function_template):
    results = []

    page_size = 2000
    page_offset = 0
    function_body = function_template.format(page_size = page_size, page_offset = page_offset)

    response = request_object(function_body)
    response_text = response.text

    while response_text != '[]':
        results.append(response_text)

        page_offset = page_offset + page_size
        function_body = function_template.format(page_size = page_size, page_offset = page_offset)
        
        response = request_object(function_body)
        response_text = response.text

    return results

def get_object_data(function_template, source_object, **kwargs):
    field_list = get_object_fields(source_object)

    function_body = function_template.format(\
        source_object = source_object, \
        field_list = field_list, \
        page_size = "{page_size}", \
        page_offset = "{page_offset}", \
        **kwargs)

    results = request_object_data(function_body)

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
