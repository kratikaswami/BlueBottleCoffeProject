import requests, csv, os.path, sys, time

url_prefix = "https://api.darksky.net/forecast"
access_key = ""             # your account's access key
lattitude = "37.831106"
longitude = "-122.254110"
origin_ts = 1451635200        # epoch time representing Jan 1, 2016 at midnight in America/LosAngeles Time Zone
url = ""
file_name = 'date_temp_file.csv'

# if file does not already exist, create the file
if os.path.isfile(file_name) != True:
    try:
        with open(file_name, mode='w') as temp_file:
            csv_writer = csv.writer(temp_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(['Time', 'Temperature'])    # write the column names of the file
    except IOError:
        print("IO Error occurred while creating the file.")


try:
    with open(file_name, mode='a') as temp_file:
        csv_writer = csv.writer(temp_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for i in range(0, 1000):            # 1000 limit because the API allows max 1000 calls a day. Had to create multiple accounts for all the data
            ts = origin_ts + i * 3600       # adding an hour to the time to each iteration
            vars = [lattitude, longitude, str(ts)]
            uri_elements = [url_prefix, access_key, ",".join(vars)]
            url = "/".join(uri_elements)    # forms the GET request url

            if url != "":
                r = requests.get(url)
                if r.status_code == requests.codes.ok:          # if response is http status 200
                    resp_json = r.json()
                    # converting date format in API response to be compatible with morse.csv format
                    time_val = time.strftime('%m/%d/%Y %H:%M:%S', time.localtime(resp_json["currently"]["time"]))
                    # writing the temperature and time to the csv
                    csv_writer.writerow([time_val,resp_json["currently"]["temperature"]])
                else:
                    print("Received non 200 response. HTTP status received: " + str(r.status_code))
                    r.raise_for_status()
except IOError:
    print("IO Error occurred while writing times and temperatures to file.")
