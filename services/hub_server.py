from os import path as Path, makedirs, remove, getpid, kill
from json import loads as jloads, dumps as jdumps
from signal import SIGUSR1
from datetime import datetime

#pip install aiohttp aiofiles psutil
from aiohttp import web
from aiofiles import open as aopen
from psutil import process_iter, NoSuchProcess, AccessDenied

# 1. 建立一個異步 Web 伺服器運行在 8081 埠。
# 2. 實作 GET /data?symbol_year 路由處理快取讀取。
# 3. 實作 POST /api/commit 路由接收 Worker 回報。
# 4. 實作發送 SIGUSR1 給 Worker 的邏輯。

# DBRoot 的目錄結構範例如下:
#	2330_2024.json
#	2330_2025.json
#	2330_2026.db
#	1201_2026.db ......
# 檔案對應到年度與目標(股票)代碼，當年度的資料以 sqlite 保存，跨年度資料以 JSON 格式保存。
# JSON 格式為長度366的物件串列對應到該年的每一天，沒資料填 NULL，
# 物件應包含至少 C O H L V (close,open,high,low,volumn) 等五的屬性
# JSON 轉換至該年度的標格應包含 D C O H L V X (date,close,open,high,low,volumn,extra data) 七個欄位，
# D 為索引欄 (也是使用該年度第幾天的整數型態即可), X 欄為可變程度字串，
# 內容為扣除 C O H L V 屬性後剩餘資料的 JSON 表示。

from MySQLite import DB

class HubServer(DB):
	def __init__(self):
		# {{{
		# 資料庫根目錄自動決定
		# 初始化 SQLite 資料庫結構 (任務追蹤表)
		# symbol: 目標 ID  # year: 資料年度
		# tid: 資料型態    # status: 擷取狀態 Pending > Running > 1 > 2 > 3 ... > Failed
		super().__init__("db/syncer.db",("tasks","""
task_id INTEGER PRIMARY KEY AUTOINCREMENT,
symbol TEXT,
year INTEGER,
tid INTEGER,
status TEXT,
created_at DATETIME DEFAULT CURRENT_TIMESTAMP
"""))

		self.doc_root = "docs/"
		if not Path.exists(self.doc_root):
			makedirs(self.doc_root)

		# 運行中 syncer 的資料表
		self.max_retry = 3
		self.max_db_caches = 3 # 可允許最大同時開啟的當年度快取資料庫數量
		self.db_caches = {} 

		# 初始化 SQLite 資料庫結構 (TOI)
		# TOI Records       # TID: Target ID    # UID: User ID
		# DATE: Date of TOI # TAG: Type of TOI
		self.toi_db=DB(Path.join(self.db_root,'toi.db'), ("records","""
RID INTEGER PRIMARY KEY AUTOINCREMENT,
TID TEXT, UID TEXT,
DATE TEXT, TAG TEXT
"""))
		# }}}

	def get_all_worker_pids(script_name="worker_app.py"):
		# {{{
		pids = []
		for proc in process_iter(['pid', 'cmdline']):
			try:
				# 檢查命令列中是否包含目標腳本名稱
				cmdline = proc.info.get('cmdline')
				if cmdline and any(script_name in s for s in cmdline):
					if proc.info['pid'] != getpid(): # 排除 Hub 自身
						pids.append(proc.info['pid'])
			except (NoSuchProcess, AccessDenied):
				continue
		# }}}
		return pids

	def notify_workers():
		# {{{
		worker_pids = get_all_worker_pids()
	
		if not worker_pids:
			print("沒有偵測到任何運作中的 Worker。")
			return

		print(f"偵測到 {len(worker_pids)} 個 Worker 實例: {worker_pids}")
	
		for pid in worker_pids:
			try:
				kill(pid, SIGUSR1)
			except ProcessLookupError:
				print(f"PID {pid} 已消失")
		# }}}

	async def db2json(self, db_name, json_path=None):
		""" 將 db 儲存的年度資料表格轉換成 JSON 檔案，並回傳 List """
		# {{{
		db_path = Path.join(self.db_root, db_name)
		data_list = [None] * 366

		# 取得所有欄位
		for r in self.query("SELECT D, C, O, H, L, V, X FROM price_data ORDER BY D ASC").ROWS:
			d_idx = r[0] # D 是索引 (第幾天)
			if 0 <= d_idx < 366:
				# 解析 X (Extra data) JSON 字串並合併
				extra = jloads(r[6]) if r[6] else {}
				day_data = { "C": r[1], "O": r[2], "H": r[3], "L": r[4], "V": r[5] }
				day_data.update(extra)
				data_list[d_idx] = day_data

		if json_path:
			full_json_path = Path.join(self.db_root, json_path)
			async with aopen(full_json_path, mode='w') as f:
				await f.write(jdumps(data_list))
			# 轉換完成後刪除 DB
			if Path.exists(db_path):
				remove(db_path)

		# }}}
		return data_list

	async def schedule_task(self, symbol, year, tid):
		"""設定 Syncer 資料並觸發之"""
		if None == self.query(
			"SELECT task_id FROM tasks WHERE symbol=? AND year=? AND tid=?",
			(symbol, year, tid)
		).FOUND : # 檢查是否已有相同任務在進行或待命
			# 加入任務
			self.commit(
				"INSERT INTO tasks (symbol, year, tid, status) VALUES (?, ?, ?, 'Pending')",
				(symbol, year, tid)
			)
			notify_workers()
			return True
		return False

	async def prepare_data(self, symbol, year):
		"""準備要回應請求的資料"""
		# {{{
		this_year = datetime.now().year
		json_file = f"{symbol}_{year}.json"
		db_file = f"{symbol}_{year}.db"

		json_path = Path.join(self.db_root, json_file)
		db_path = Path.join(self.db_root, db_file)

		if year != this_year:
			if Path.exists(json_path):
				async with aopen(json_path, mode='r') as f:
					return jloads(await f.read())

			if Path.exists(db_path):
				return await self.db2json(db_file, json_file)

			await self.schedule_task(symbol, year, 0)
			return "Pending"
		else:
			# 當年度處理
			max_date = 0
			if Path.exists(db_path):
				max_date = datetime.fromtimestamp(Path.getmtime(db_path)).timetuple().tm_yday
			today_day_of_year = datetime.now().timetuple().tm_yday
			if Path.exists(db_path) and max_date >= today_day_of_year - 1:
				return await self.db2json(db_file) # 不轉 json 檔，因為還會更新

			await self.schedule_task(symbol, year, max_date)
			return "Pending"
		# }}}

	async def handle_get_data(self, request):
		"""處理前端資料請求: ?2330.TW-2025"""
		# {{{
		query = request.query_string
		try:
			tid, yid = query.split('-')
			data = await self.prepare_data(tid, int(yid))
			return web.json_response({"status": "Success", "data": data})
		except Exception as e:
			return web.json_response({"status": "Error", "message": str(e)}, status=400)
		# }}}

	async def handle_list_task(self, request):
		"""處理任務追蹤表的資料列出請求"""
		return web.json_response({
			"status": "Success",
			"tasks": self.query("SELECT task_id, symbol, year, tid, status, created_at FROM tasks ORDER BY created_at DESC").DICT
		})

	async def handle_get_task(self, request):
		"""/api/request 處理 Worker 任務要求"""
		TASKS = ["yf","learn3"]
		type_c = request.query.get("taskType") or '0'
		type_c = type_id.split(",")
		type_c = " OR ".join([f"tid={v}" for v in type_c])

		row = self.query(
			"SELECT task_id, symbol, year FROM tasks WHERE status = 'Pending' AND ("+type_c+") LIMIT 1", 
		).FOUND
		if None == row :
			return web.json_response({})

		task_id, symbol, year = row
		self.commit(
			"UPDATE tasks SET status='Running' WHERE task_id=?",
			(task_id,)
		);
		return web.json_response({
			"TaskID": task_id,
			"Script": TASKS[int(type_id)],
			"Args": { "Symbol": symbol, "Year": year, "Interval": "1d" }
		})

	async def handle_commit_task(self, request):
		"""/api/commit 處理 Worker 任務回報"""
		task_id = request.query.get("taskID")
		data = await request.json() # {data} or "FAILED"
		if not data :
			return web.json_response({"status": "Ignored"})

		task_info = self.query(
			"SELECT symbol, year, tid FROM tasks WHERE task_id=?",
			(task_id,)
		).FOUND
		if not task_info :
			return web.json_response({"status": "Error", "message": "Task not found"}, status=404)

		symbol, year, task_id = task_info
		if data == "FAILED":
			status = self.query(
				"SELECT status FROM tasks WHERE task_id=?",
				(task_id,)
			).FOUND
			rc = int(status) if status.isdigit() else 0
			if rc < self.max_retry:
				self.commit("UPDATE tasks SET status=? WHERE task_id=?", (str(rc + 1), task_id))
				return web.json_response({"status": "Retrying"})
			#self.commit("DELETE FROM tasks WHERE task_id=?", (task_id,))
			self.commit("UPDATE tasks SET status=? WHERE task_id=?", ("Failed", task_id))
		else:
			# 寫入資料到對應的股票 DB
			db_name = f"{symbol}_{year}.db"
			db_path = Path.join(self.db_root, db_name)
			
			cmds = []
			count = -1
			for item in data:
				count += 1
				if item == 0 : continue
				# 這裡假設 Worker 回傳格式符合表格欄位
				# 處理 X 欄位：扣除基本欄位後轉 JSON
				base_keys = {'D', 'C', 'O', 'H', 'L', 'V'}
				extra_data = {k: v for k, v in item.items() if k not in base_keys}
				for k in base_keys :
					if k not in item : item[k] = 0
				item['D'] = count
				cmds.append((
					"INSERT OR REPLACE INTO price_data (D, C, O, H, L, V, X) VALUES (?,?,?,?,?,?,?)",
					(item['D'],item['C'],item['O'],item['H'],item['L'],item['V'],jdumps(extra_data))
				))
			
			DB(db_path, ("price_data","""
				D INTEGER PRIMARY KEY,
				C REAL, O REAL,
				H REAL, L REAL,
				V INTEGER, X TEXT
			""")).commit(cmds)

			self.commit("DELETE FROM tasks WHERE task_id=?", (task_id,))

			# 如果是往年資料，立即轉為 JSON
			if year < datetime.now().year:
				await self.db2json(db_name, f"{symbol}_{year}.json")
		return web.json_response({"status": "Acknowledged"})

	async def handle_get_toi(self, request):
		"""處理前端資料請求: GET ?tid=2330.TW&uid=0"""
		try:
			tid = request.query_string
			return web.json_response({
				"status": "Success",
				"data": self.toi_db.query("SELECT * FROM records WHERE TID=?", (tid,)).DICT
			})
		except Exception as e:
			return web.json_response({"status": "Error", "message": str(e)}, status=400)

	async def handle_commit_toi(self, request):
		"""/tapi/commit 處理 使用者 TOI 上傳 POST ?tid=2330.TW&uid=0 [{TID,UID,DATE,TAG}]"""
		tid = request.query.get("tid")
		uid = request.query.get("uid")

		data = await request.json() # {data} or "FAILED"
		if not data :
			return web.json_response({"status": "Ignored"})
		
		cmds=[]
		for row in data :
			dv={"TID":tid, "UID":uid, "DATE":"", "TAG":""}
			dv.update(row);
			cmds.append((
				"INSERT OR REPLACE INTO price_data (TID,UID,DATE,TAG) VALUES (?,?,?,?)",
				(dv['TID'],dv['UID'],dv['DATE'],dv['TAG'])
			))
		self.toi_db.execute(cmds);
		return web.json_response({"status": "Acknowledged"})

	def make_app(self):
		app = web.Application()
		app.router.add_get('/dapi', self.handle_get_data)			# 財金數據快取服務
		app.router.add_post('/api/list', self.handle_list_task)		# 取得工作列表
		app.router.add_post('/api/request', self.handle_get_task)	# 取得工作
		app.router.add_post('/api/commit', self.handle_commit_task)	# 回覆工作結果
		app.router.add_get('/tapi', self.handle_get_toi)			# TOI delivery
		app.router.add_post('/tapi/commit', self.handle_commit_toi)	# TOI collect
		app.router.add_static('/', self.doc_root, show_index=False)	# 靜態檔案分享 (show_index=True 允許列出目錄)
		return app

if __name__ == "__main__":
	hub = HubServer()
	app = hub.make_app()
	print("Server starting at http://localhost:8081")
	web.run_app(app, port=8081)
