import yfinance as yf

if __name__ == "__main__":  
  tickets = ["AAPL", "META", "9021.T"]
  
  for ticket in tickets:
    data = yf.download(ticket, period="max")
    data.to_csv(f"./batch_layer/storage/raw/{ticket}.csv")