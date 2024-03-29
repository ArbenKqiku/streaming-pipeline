import requests
import json

url = "https://stream.companieshouse.gov.uk/"

try:
    response=requests.get(
        url=url+"companies",
        headers={
            "Authorization": 'Basic ZTA1YjQzYmYtNDY4OC00YWFjLWIxZWItOTY2MWUxYzZjMDA0Og==',
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        },
        stream=True
    )

    print("Established connection to Company House UK streaming API")

    for line in response.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')

            json_line = json.loads(decoded_line)

            # Build empty JSON
            company_profile = {
                "company_name": None,
                "company_number": None,
                "company_status": None,
                "date_of_creation": None,
                "postal_code": None,
                "published_at": None
            }

            try:
                company_profile['company_name'] = json_line["data"]["company_name"]
            except KeyError:
                company_profile['company_name'] = "NA"

            try:
                company_profile['company_number'] = json_line["data"]["company_number"]
            except KeyError:
                company_profile['company_number'] = "NA"
            
            try:
                company_profile['company_status'] = json_line["data"]["company_status"]
            except KeyError:
                company_profile['company_status'] = "NA"

            try:
                company_profile["date_of_creation"] = json_line["data"]["date_of_creation"]
            except KeyError:
                company_profile["date_of_creation"] = "NA"

            try:
                company_profile["postal_code"] = json_line["data"]["registered_office_address"]["postal_code"]
            except KeyError:
                company_profile["postal_code"] = "NA"

            try:
                company_profile["published_at"] = json_line["event"]["published_at"]
            except KeyError:
                company_profile["published_at"] = "NA"

            print("")
            print("BREAK")
            print("")
            print(company_profile)

except Exception as e:
    print(f"an error occurred {e}")

