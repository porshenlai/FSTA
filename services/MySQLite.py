from sqlite3 import connect
from os import path as Path, makedirs

class DB:
	def __init__ (self, dbf, layout=None) :
		self.db = dbf
		self.db_root = Path.dirname(dbf)
		self.cursor = None
		if not Path.exists(self.db_root) :
			makedirs(self.db_root)
		if layout :
			self.commit(f"CREATE TABLE IF NOT EXISTS {layout[0]} ({layout[1]})");

	# cursor.execute("SELECT task_id FROM tasks WHERE symbol=? AND year=? AND tid=?", (symbol, year, tid))
	def query(self, *args) :
		self.cursor = None
		with connect(self.db) as conn:
			cursor = conn.cursor()
			cursor.execute(*args);
		self.cursor = cursor;
		return self

	# "INSERT INTO tasks (symbol, year, tid, status) VALUES (?, ?, ?, 'Pending')", (symbol, year, tid)
	def commit(self, *cmds) :
		if str == type(cmds[0]) : cmds=[cmds]
		with connect(self.db) as conn:
			for cmd in cmds :
				conn.execute(*cmd)
			conn.commit()
		return self

	@property
	def FOUND (self) :
		if not self.cursor : return False
		row = cursor.fetchone()
		return None if not row else dict(row)

	@property
	def ROWS (self) :
		if not self.cursor : return None
		return self.cursor.fetchall()

	@property
	def DICT (self) :
		if not self.cursor : return None
		rows = self.cursor.fetchall()
		return [dict(row) for row in rows] # 將 sqlite3.Row 轉為 list of dict
