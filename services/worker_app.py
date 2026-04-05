import asyncio
import signal
import sys
import os
import json
import aiohttp
#pip install pyfinance

# 1. 監聽 SIGUSR1 信號。
# 2. 接收信號後，透過 API 向 Hub 索取任務。
# 3. 執行完畢後透過 POST API 回報結果。

class WorkerApp:
	def __init__(self, argv, hub_url="http://localhost:8081"):
		self.argv = argv
		self.hub_url = hub_url
		self.is_processing = False
		self.session = None
		self.timeout = aiohttp.ClientTimeout(total=30)
		self.type = "0"

	async def _get_session(self):
		if self.session is None or self.session.closed:
			self.session = aiohttp.ClientSession(timeout=self.timeout)
		return self.session

	def handle_signal(self):
		"""SIGUSR1 信號處理 (Linux)"""
		print(f"\n[Signal] 收到信號，準備檢查 Hub 任務池...")
		self.trigger_tasks()

	def trigger_tasks(self):
		if not self.is_processing:
			asyncio.create_task(self.process_tasks())
		else:
			print("[Info] 任務處理中，忽略本次觸發。")

	async def process_tasks(self):
		self.is_processing = True
		session = await self._get_session()
		
		try:
			while True:
				try:
					async with session.post(
						f"{self.hub_url}/api/request",
						params={"taskType": self.type} 
					) as resp:
						if resp.status != 200:
							break
						task = await resp.json()
						print("DEBUG-TASK",task);
				except Exception as e:
					print(f"[Network Error] 無法連線至 Hub: {e}")
					break
					
				if not task or "TaskID" not in task:
					break

				task_id = task.get("TaskID")
				script_name = task.get("Script")
				args = task.get("Args", {})

				print(f"[*] 領取任務 {task_id} -> {script_name}")

				script_path = [os.path.join("syncer", f"{script_name}.py")]
				
				if not os.path.exists(script_path[0]):
					output, returncode = f"Error: {script_path} not found.", 1
				else:
					for kn in args :
						script_path.append(kn+"="+str(args[kn]))

					# 使用 subprocess 執行，並捕獲輸出
					process = await asyncio.create_subprocess_exec(
						sys.executable, *script_path, "",
						stdout=asyncio.subprocess.PIPE,
						stderr=asyncio.subprocess.PIPE
					)
					stdout, stderr = await process.communicate()
					output = stdout.decode().strip() or stderr.decode().strip()
					if output != "FAILED" :
						output = json.loads(output)
					print("DEBUG",output);
					returncode = process.returncode

				# 3. 回報結果 (增加簡單的失敗重試)
				for _ in range(3): 
					try:
						async with session.post(
							f"{self.hub_url}/api/commit", 
							params={"taskID": task_id}, 
							json=output
						) as resp:
							if resp.status == 200:
								break
					except:
						await asyncio.sleep(1)
				break
		finally:
			self.is_processing = False
			print("[Worker] 進入休眠模式。")

	async def main(self):
		loop = asyncio.get_running_loop()
		print(f"Worker PID: {os.getpid()} | Hub: {self.hub_url}")

		if sys.platform != "win32":
			loop.add_signal_handler(signal.SIGUSR1, self.handle_signal)
		else:
			# 開發環境模擬
			async def debug_input() :
				while True :
					await loop.run_in_executor(None, sys.stdin.readline)
					self.trigger_tasks()
			asyncio.create_task(debug_input())

		self.trigger_tasks() # 啟動時自檢
		
		while True:
			await asyncio.sleep(3600)
			break

if __name__ == "__main__":
	try:
		argv={"type":"0"};
		for arg in sys.argv[1:] : 
			arg=arg.split("=");
			argv[arg[0]]="=".join(arg[1:])
		asyncio.run(WorkerApp(argv).main())
	except KeyboardInterrupt:
		pass
