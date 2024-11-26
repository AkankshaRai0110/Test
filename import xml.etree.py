import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import cx_Oracle
from configparser import ConfigParser

def get_organization_data(cursor, icao_code):
    cursor.execute("""
        SELECT 
            country AS COUNTRY_CODE,
            ICAO_CD AS IDENTITY,
            LNG AS LANGUAGE_CODE,
            ORG_NM AS NAME,
            ICAO_CD AS OLD_IDENTITY,
            ORG_TYPE AS OWNER_ATTRIBUTE,
            STATUS AS OWNER_CODE_STAT,
            REGION AS REGION,
            ZONE AS ZONE,
            CASE 
                WHEN ORG_TYPE IN ('AIRLINE', 'AIRFRAMER', 'BUSINESS') THEN 'COMMERCIAL'
                WHEN ORG_TYPE = 'MILITARY' THEN 'MILITARY'
                ELSE NULL
            END AS OWNER_TYPE_ID,
            CASE 
                WHEN ORG_TYPE NOT IN ('AIRLINE', 'AIRFRAMER', 'BUSINESS', 'MILITARY') THEN 'The ORG_TYPE ' || ORG_TYPE || ' given is not allowed to update in IFS'
                ELSE NULL
            END AS ERROR_MESSAGE
        FROM 
            C_BO_ORGANIZATION
        WHERE 
            ICAO_CD = :icao_code
    """, icao_code=icao_code)
    organization = cursor.fetchone()
    # print(organization)
    # for x in organization:
    #      print(x)
    return organization

def generate_xml():
    # Read database connection details from config.txt
    config = ConfigParser()
    config.read('config.txt')
    dsn_tns = config.get('DATABASE DETAILS', 'dsn_tns')
    user = config.get('DATABASE DETAILS', 'user')
    password = config.get('DATABASE DETAILS', 'password')

    # Connect to the Oracle database
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns) 
    cursor = connection.cursor()
    icao_code = input("Enter ICAO code: ")
    organization = get_organization_data(cursor, icao_code)
    
    if organization:

        # create soap envelope element
        envelope = ET.Element("SOAP-ENV:Envelope", attrib={"xmlns:SOAP-ENV":"http://schemas.xmlsoap.org/soap/envelope/"})

        # Create the Header element
        header = ET.SubElement(envelope, "SOAP-ENV:Header")

        # Child element of header-message
        message = ET.SubElement(header, "fndcn:Message", attrib={"xmlns:fndcn":"urn:ifsworld-com:schemas:fndcn",
                                                                  "SOAP-ENV:mustUnderstand":"1"})

         # start date
        SentAt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # expiry date= 
        ExpiresAt = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
        #Child elements of message with some hardcoded values
        message_data = {
            "Type": "HTTP_TEST",
            "Function": "RECEIVE_OWNER_OPERATOR_INFO",
            "Sender": "HttpTester",
            "Receiver": "HttpClient",
            "SentAt": SentAt,
            "ExpiresAt": ExpiresAt
        }
        # print(type(message_data))-dictionary
        for key, value in message_data.items():
            childmessage = ET.SubElement(message, "fndcn:" + key)
            childmessage.text = value

        # Body element
        body = ET.SubElement(envelope, "SOAP-ENV:Body")
        # Child element of body-owner_operator_info
        owner_operator_info = ET.SubElement(body, "OWNER_OPERATOR_INFO", attrib={"xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance"})
      
        
        owner_operator_info_data = {
                "C1": "",
                "C2": "",
                "C3": "",
                "C4": "",
                "C5": "",
                "COUNTRY_CODE": organization[0],
                "EXISTS": "",
                "IDENTITY": organization[1],
                "LANGUAGE_CODE": organization[2],
                "NAME": organization[3],
                "OLD_IDENTITY": organization[4],
                "OPERATOR_TYPE": "",
                "OWNER_ATTRIBUTE": organization[5],
                "OWNER_CODE_STAT": organization[6],
                "OWNER_TYPE_ID": organization[9],
                "RECEIVER": "",
                "REGION": organization[7],
                "REQUEST_ID": "",
                "SENDER": "GEAMDM",
                "TIMESTAMP": SentAt,
                "TRANSACTION_ID": "BDD-2",
                "ZONE": organization[8]
            }
        for key, value in owner_operator_info_data.items():
                childowner_operator_info_data = ET.SubElement(owner_operator_info, key)
                childowner_operator_info_data.text = value

        #write xml into genxml.xml
        tree = ET.ElementTree(envelope)
        tree.write("genxml.xml", xml_declaration=True, encoding='utf-8')

    else:
        print(f"No organization found with ICAO code: {icao_code}")

    cursor.close()
    connection.close()

generate_xml()