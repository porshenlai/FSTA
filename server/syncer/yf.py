import sys
import json
import requests
import yfinance as yf
from datetime import datetime

## gemini-helpme: 
## 1. 解析傳入的 Task JSON。
## 2. 使用 yfinance 抓取指定年份數據。
## 3. 格式化為 [0, {"O":...}] 格式。
## 4. 透過 Callback URI 回報結果。

def main():
	args={}
	for arg in sys.argv[1:] :
		if not arg : continue
		arg=arg.split("=")
		args[arg[0]]=arg[1]

	print(f"正在處理任務 {args}", file=sys.stderr)

	try:
		# 抓取數據
		ticker = yf.Ticker(args['Symbol'])
		start_date = f"{args['Year']}-01-01"
		end_date = f"{args['Year']}-12-31"
		df = ticker.history(start=start_date, end=end_date, interval=args['Interval'])

		# 格式化數據：建立 366 天的串列 (預填 0)
		annual_data = [0] * 367 
		for date, row in df.iterrows():
			day_of_year = date.timetuple().tm_yday
			annual_data[day_of_year] = {
				"O": round(row['Open'], 2),
				"C": round(row['Close'], 2),
				"H": round(row['High'], 2),
				"L": round(row['Low'], 2),
				"V": int(row['Volume'])
			}

		print("COMPLETED",file=sys.stderr)
		print(json.dumps(annual_data,ensure_ascii=False))

	except Exception as e:
		# requests.post(task['Callback']['URI'], json={"status": "Fail", "error": str(e)})
		print(f"FAILED: {e}",file=sys.stderr)
		print("FAILED")

if __name__ == "__main__":
	main()
