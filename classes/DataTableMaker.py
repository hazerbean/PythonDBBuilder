import pandas as pd
import json
import requests

class DataTableMaker:
    def ArrayOfJsons(self, jsonInput):
        for element in jsonInput:
            print(element)
        print(type(jsonInput))
        return jsonInput
