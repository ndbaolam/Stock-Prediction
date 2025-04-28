import requests

symbol = "VNM"
url = f"https://finfo-api.vndirect.com.vn/v4/stock_prices?q=code:{symbol}~date:gte:2024-01-01&size=100"
response = requests.get(url)
print(response.json())
