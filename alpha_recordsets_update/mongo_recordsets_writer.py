import os
from typing import Optional, Tuple
from datetime import datetime

try:
	from pymongo import MongoClient
	from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
except ImportError:
	MongoClient = None  # type: ignore

import logging
logger = logging.getLogger(__name__)


class _TargetClient:
	def __init__(self, uri: str, db_name: str, coll_name: str):
		self.uri = uri
		self.db_name = db_name
		self.coll_name = coll_name
		self.client: Optional[MongoClient] = None
		self._connect()

	def _connect(self):
		if not MongoClient:
			return
		try:
			self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
			self.client.admin.command('ping')
			logger.info("Connected to MongoDB: %s %s.%s", self.uri, self.db_name, self.coll_name)
		except (ConnectionFailure, ServerSelectionTimeoutError) as e:
			logger.error("Mongo connect failed: %s", e)
			self.client = None

	def _merge_records(self, existing: list, incoming: list) -> list:
		"""Merge two lists of [key, ...] rows by first column key; prefer incoming on conflict."""
		if not existing:
			return list(incoming)
		index = {row[0]: row for row in existing if row and len(row) > 0}
		for row in incoming:
			if row and len(row) > 0:
				index[row[0]] = row
		merged = [index[k] for k in sorted(index.keys())]
		return merged

	def upsert_recordset(self, alpha_id: str, recordset_name: str, data: dict, date_range: Tuple[str, str], user_id: str, user_key: str, update_mode: str = "update"):
		if not self.client:
			return
		start, end = date_range
		coll = self.client[self.db_name][self.coll_name]
		doc = coll.find_one({"id": alpha_id}, {f"recordsets.{recordset_name}": 1}) or {}
		existing = (((doc.get("recordsets") or {}).get(recordset_name) or {}).get("records") or [])
		incoming = data.get("records", [])
		if update_mode == "skip" and existing and len(existing) > 0:
			logger.info("Skip update for alpha=%s recordset=%s (mode=skip and existing present)", alpha_id, recordset_name)
			return
		if update_mode == "overwrite":
			merged = list(incoming)
		else:
			merged = self._merge_records(existing, incoming)
		if merged:
			new_start, new_end = str(merged[0][0]), str(merged[-1][0])
		else:
			new_start, new_end = start, end
		prev_meta = (doc.get("recordsets") or {}).get(recordset_name) or {}
		if prev_meta.get("start") == new_start and prev_meta.get("end") == new_end and len(existing) == len(merged):
			logger.info("No change for alpha=%s recordset=%s; skipping DB update.", alpha_id, recordset_name)
			return
		update_doc = {
			"$set": {
				"id": alpha_id,
				f"recordsets.{recordset_name}.schema": data.get("schema"),
				f"recordsets.{recordset_name}.records": merged,
				f"recordsets.{recordset_name}.start": new_start,
				f"recordsets.{recordset_name}.end": new_end,
				f"recordsets.{recordset_name}.updated_at": datetime.utcnow().isoformat() + "Z",
			}
		}
		coll.update_one({"id": alpha_id}, update_doc, upsert=True)

	def close(self):
		if self.client:
			try:
				self.client.close()
			except Exception:
				pass



class MongoWriters:
	def __init__(self, user_key: str, write_primary: bool = True, write_secondary: bool = False, update_mode: str = "update", primary_port: int = 27018, secondary_port: int = 27017):
		self.user_key = user_key
		self.update_mode = update_mode
		self.primary_regular: Optional[_TargetClient] = None
		self.primary_super: Optional[_TargetClient] = None
		self.secondary_regular: Optional[_TargetClient] = None
		self.secondary_super: Optional[_TargetClient] = None
		self._id_kind_cache: dict[str, str] = {}

		if write_primary and MongoClient:
			self.primary_regular = _TargetClient(
				uri=f"mongodb://localhost:{primary_port}/",
				db_name=f"wq{user_key}",
				coll_name="regular_alphas",
			)
			self.primary_super = _TargetClient(
				uri=f"mongodb://localhost:{primary_port}/",
				db_name=f"wq{user_key}",
				coll_name="super_alphas",
			)
		if write_secondary and MongoClient:
			self.secondary_regular = _TargetClient(
				uri=f"mongodb://localhost:{secondary_port}/",
				db_name="regular_alphas",
				coll_name=user_key,
			)
			self.secondary_super = _TargetClient(
				uri=f"mongodb://localhost:{secondary_port}/",
				db_name="super_alphas",
				coll_name=user_key,
			)

	def _detect_kind(self, alpha_id: str) -> str:
		# cached
		kind = self._id_kind_cache.get(alpha_id)
		if kind:
			return kind
		# Prefer detection from primary DB
		try:
			if self.primary_regular and self.primary_regular.client:
				c = self.primary_regular.client[f"wq{self.user_key}"]["regular_alphas"]
				if c.find_one({"id": alpha_id}, {"_id": 1}):
					self._id_kind_cache[alpha_id] = "regular"
					return "regular"
			if self.primary_super and self.primary_super.client:
				c = self.primary_super.client[f"wq{self.user_key}"]["super_alphas"]
				if c.find_one({"id": alpha_id}, {"_id": 1}):
					self._id_kind_cache[alpha_id] = "super"
					return "super"
		except Exception:
			pass
		# default
		self._id_kind_cache[alpha_id] = "regular"
		return "regular"

	def upsert_recordset(self, alpha_id: str, user_id: str, recordset_name: str, data: dict, date_range: Optional[Tuple[str, str]]):
		kind = self._detect_kind(alpha_id)
		
		# Ensure date_range is not None for _TargetClient.upsert_recordset
		safe_date_range = date_range if date_range else ("", "")

		if kind == "super":
			if self.primary_super:
				self.primary_super.upsert_recordset(alpha_id=alpha_id, recordset_name=recordset_name, data=data,
										date_range=safe_date_range, user_id=user_id, user_key=self.user_key, update_mode=self.update_mode)
		else:
			if self.primary_regular:
				self.primary_regular.upsert_recordset(alpha_id=alpha_id, recordset_name=recordset_name, data=data,
										date_range=safe_date_range, user_id=user_id, user_key=self.user_key, update_mode=self.update_mode)
		if kind == "super":
			if self.secondary_super:
				self.secondary_super.upsert_recordset(alpha_id=alpha_id, recordset_name=recordset_name, data=data,
										date_range=safe_date_range, user_id=user_id, user_key=self.user_key, update_mode=self.update_mode)
		else:
			if self.secondary_regular:
				self.secondary_regular.upsert_recordset(alpha_id=alpha_id, recordset_name=recordset_name, data=data,
										date_range=safe_date_range, user_id=user_id, user_key=self.user_key, update_mode=self.update_mode)

	def close(self):
		for c in (self.primary_regular, self.primary_super, self.secondary_regular, self.secondary_super):
			if c:
				c.close()
