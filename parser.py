import json
import os
from multiprocessing import Manager
from concurrent import futures
from datetime import datetime
from functools import partial
from typing import List, Dict, IO, Tuple, Awaitable

import math
import sys
import time

MAX_CHUNK_SIZE = 750000
DUMP_FILE = "records.json"


class LogParser:
	def __init__(self, source_file):
		self.source_file = source_file
		self.cache = Manager().dict()

	@staticmethod
	def _range_intersects_chunk(start: datetime, end: datetime, chunk_start: datetime, chunk_end: datetime) -> bool:
		return (start <= chunk_start <= end) or \
			(chunk_start <= start <= end <= chunk_end) or \
			(start <= chunk_end <= end)

	@staticmethod
	def _line_begin_seek_offset(f: IO, pos: int) -> int:
		"""
		Given the current seek position on a line in a file, move back to the beginning
		of that line

		:return: The seek position for the beginning of the current line
		"""
		if pos <= 0:
			return pos

		f.seek(pos - 1)
		val = f.read(1)

		while val.decode("utf-8") != "\n":
			try:
				f.seek(-2, os.SEEK_CUR)
				val = f.read(1)
			except OSError:
				return 0

		return f.tell()

	@staticmethod
	def _resolve_filtered_records(promises: List[Awaitable[List[Dict]]], resolve_to: str) -> int:
		"""
		Given a list of future objects, resolve them and dump the resolved records as JSON
		into the dump file.

		return: Total number of records resolved from the futures object
		"""
		total = 0

		with open(resolve_to, "a", encoding='utf-8') as resolve_f:
			resolve_f.write("[")

			for promise in futures.as_completed(promises):
				records = promise.result()
				total += len(records)

				if records:
					resolve_f.write(json.dumps(records)[1:-1])
					resolve_f.write(",")

			resolve_f.seek(resolve_f.tell() - 1, os.SEEK_SET)
			resolve_f.truncate()
			resolve_f.write("]")

		return total

	def _should_process_chunk(
			self, f: IO, chunk_id: int, begin_ptr: int,
			end_ptr: int, start: datetime, end: datetime) -> bool:
		"""
		Given a chunk of a file, checks whether any part of the range[start, end] falls within this chunk.

		:return: Boolean, indicating whether or not the current chunk should be processed
		"""
		if chunk_id in self.cache:
			return self._range_intersects_chunk(start, end, *self.cache[chunk_id])

		# move to the beginning of the line
		begin_ptr_offset = self._line_begin_seek_offset(f, begin_ptr)
		f.seek(begin_ptr_offset)

		# if the current line had been processed by the prev chunk, skip this line
		if chunk_id > 0 and begin_ptr_offset >= self._line_begin_seek_offset(f, begin_ptr - 1):
			f.readline()

		chunk_start_date, _, _ = f.readline().decode("utf-8").split(" ")
		chunk_start_date = datetime.fromisoformat(chunk_start_date[:-1])

		f.seek(self._line_begin_seek_offset(f, end_ptr))

		chunk_end_date, _, _ = f.readline().decode("utf-8").split(" ")
		chunk_end_date = datetime.fromisoformat(chunk_end_date[:-1])

		self.cache[chunk_id] = [chunk_start_date, chunk_end_date]
		return self._range_intersects_chunk(start, end, chunk_start_date, chunk_end_date)

	def _remove_trailing_empty_line(self, f: IO, end_ptr: int) -> None:
		"""
		remove the empty line from the end of the file
		"""
		f.seek(self._line_begin_seek_offset(f, end_ptr))
		end_line = f.readline().decode("utf-8")

		if not end_line:
			with open(self.source_file, "a") as f_w:
				f_w.seek(0, os.SEEK_END)
				f_w.seek(f.tell() - 1, os.SEEK_SET)
				f_w.truncate()

	def _process_chunk(self, num_chunks: int, start: datetime, end: datetime, chunk_id: int) -> List[Dict]:
		"""
		Process the chunk only if it is of our interest, else skip this altogether

		:return: number of records of in the range[start, end] that fell within the bounds of this chunk
		"""
		begin_ptr = chunk_id * MAX_CHUNK_SIZE
		end_ptr = min(MAX_CHUNK_SIZE * (chunk_id + 1) - 1, os.path.getsize(self.source_file))
		filtered_records = []

		with open(self.source_file, 'rb') as f:
			if chunk_id == num_chunks:
				self._remove_trailing_empty_line(f, end_ptr)

			if not self._should_process_chunk(f, chunk_id, begin_ptr, end_ptr, start, end):
				return []

			# move to the beginning of the line
			begin_ptr_offset = self._line_begin_seek_offset(f, begin_ptr)
			f.seek(begin_ptr_offset)

			# if the current line had been processed by the prev chunk, skip this line
			if chunk_id > 0 and begin_ptr_offset >= self._line_begin_seek_offset(f, begin_ptr - 1):
				f.readline()

			for line in f:
				date, email, session_id = line.decode("utf-8").strip("\n").split(" ")

				if start <= datetime.fromisoformat(date[:-1]) <= end:
					filtered_records.append(dict(date=date, email=email, session_id=session_id))

				# do not consume line from the next chunk
				if f.tell() > end_ptr:
					break

		return filtered_records

	def process_range(self, start: datetime, end: datetime, dump_to: str) -> Tuple[int, float]:
		"""
		Given a start date and an end date, break the source file into multiple chunks of size
		no more than MAX_CHUNK_SIZE, spawn parallel processes to process those chunks, and filter
		the records that fall within the given range.

		:return: A 2-tuple containing the number of records filtered and total execution time
		"""
		if start > end:
			raise ValueError("start date must be less than or equal to end date")

		size = os.path.getsize(self.source_file)
		num_chunks = math.ceil(size / MAX_CHUNK_SIZE)

		t0, promises = time.time(), []
		with futures.ProcessPoolExecutor() as executor:
			process_chunk_curried = partial(self._process_chunk, num_chunks - 1, start, end)

			for chunk_id in range(num_chunks):
				promise = executor.submit(process_chunk_curried, chunk_id)
				promises.append(promise)

		total_resolved = self._resolve_filtered_records(promises, dump_to)

		exec_time = time.time() - t0
		return total_resolved, exec_time


def setup():
	args = sys.argv
	n_args = len(args)

	if n_args != 4:
		print("Invalid number of arguments provided; needed {}, got {}".format(3, n_args - 1))
		return

	source_file = args[1]
	try:
		start = datetime.fromisoformat(args[2][:-1])
		end = datetime.fromisoformat(args[3][:-1])
	except ValueError as e:
		print(e)
		return

	try:
		os.remove(DUMP_FILE)
	except FileNotFoundError:
		pass
	finally:
		parser = LogParser(source_file)
		count, t = parser.process_range(start, end, DUMP_FILE)
		print("{} records were dumped into the file '{}' in {}s".format(count, DUMP_FILE, t))


if __name__ == '__main__':
	setup()
