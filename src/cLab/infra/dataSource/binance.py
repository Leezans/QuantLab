import binance
import os
import json



class Binance:
    def __init__(self, api_key: str, secret_key: str):
        self.client = binance.Client(api_key, secret_key)   


    def get_price(self, symbol: str) -> float:
        ticker = self.client.get_ticker(symbol=symbol)
        return float(ticker['lastPrice'])



class binanceCilent:
    def __init__(self, api_key: str, secret_key: str):
        self.client = binance.Client(api_key, secret_key)   


    def get_price(self, symbol: str) -> float:
        ticker = self.client.get_ticker(symbol=symbol)
        return float(ticker['lastPrice'])   
    

    

if __name__ == "__main__":
    api_key = "your_api_key"
    



    pass