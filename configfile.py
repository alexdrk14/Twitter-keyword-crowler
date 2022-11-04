#!/usr/bin/env python


#mongo DB address/port and db/collection
DBCONFIG={
          "address"   : '127.0.0.1',   #IP address of MongoDB, like '127.0.0.1' in string type
          "port"      : 27017,  #Port of MongoDB, like 27017 in integer type
          "db"        : 'YourDBName',   #Name of MongoDB Database, like 'CollectedDatabase' in string type
          }

TWCONFIG = {"consumer_key"       : "",
            "consumer_secret"    : "",
            "access_token_key"   : "",
            'access_token_secret': ""
            }

KEYCONFIG = {"file": "keywords.txt"}
