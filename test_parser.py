import unittest

from parser import *


class TestParser(unittest.TestCase):
	def setUp(self):
		self.source_file = "sample1.txt"
		self.dump_file_mp = "records_mp.json"
		self.dump_file_seq = "records_seq.json"

	def parse_records_seq(self, source_file, start, end):
		t0, total = time.time(), 0

		with open(source_file, "rb") as f, open(self.dump_file_seq, "a") as f_:
			f_.write("[")
			for line in f:
				date, email, session_id = line.decode("utf-8").strip("\n").split(" ")
				if start <= datetime.fromisoformat(date[:-1]) <= end:
					json.dump(dict(date=date, email=email, session_id=session_id), f_, indent=4)
					f_.write(",")
					total += 1

			f_.seek(f.tell() - 1, os.SEEK_SET)
			f_.truncate()
			f_.write("]")

		return total, time.time() - t0

	def test_count_is_greater_than_zero(self):
		start, end = datetime.fromisoformat("2000-06-01T13:13:29"), datetime.fromisoformat("2001-07-03T03:32:08")
		parser = LogParser(self.source_file)
		records_count_mp, exec_time_mp = parser.process_range(start, end, self.dump_file_mp)
		print("test_count_is_greater_than_zero: Filtered {} records in {}".format(records_count_mp, exec_time_mp))
		self.assertGreater(records_count_mp, 0)

	def test_count_is_zero(self):
		start, end = datetime.fromisoformat("2022-01-01T13:13:29"), datetime.fromisoformat("2022-02-03T03:32:09")
		parser = LogParser(self.source_file)
		records_count_mp, exec_time_mp = parser.process_range(start, end, self.dump_file_mp)
		print("test_count_is_zero: Filtered {} records in {}".format(records_count_mp, exec_time_mp))
		self.assertEqual(records_count_mp, 0)

	def test_range_intersects_chunk(self):
		start, end = datetime.fromisoformat("2004-01-01T13:13:29"), datetime.fromisoformat("2004-10-03T03:32:08")

		chunk_s, chunk_e = datetime.fromisoformat("2003-12-30T13:13:29"), datetime.fromisoformat("2004-02-03T03:32:08")
		self.assertEqual(LogParser._range_intersects_chunk(start, end, chunk_s, chunk_e), True)

		chunk_s, chunk_e = datetime.fromisoformat("2003-12-30T13:13:29"), datetime.fromisoformat("2004-01-01T13:13:29")
		self.assertEqual(LogParser._range_intersects_chunk(start, end, chunk_s, chunk_e), True)

		chunk_s, chunk_e = datetime.fromisoformat("2004-10-03T03:32:08"), datetime.fromisoformat("2005-01-01T13:13:29")
		self.assertEqual(LogParser._range_intersects_chunk(start, end, chunk_s, chunk_e), True)

	def test_range_does_not_intersect_chunk(self):
		start, end = datetime.fromisoformat("2004-01-01T13:13:29"), datetime.fromisoformat("2004-10-03T03:32:08")

		chunk_s, chunk_e = datetime.fromisoformat("2003-12-28T13:13:29"), datetime.fromisoformat("2003-12-30T13:13:29")
		self.assertNotEqual(LogParser._range_intersects_chunk(start, end, chunk_s, chunk_e), True)

		chunk_s, chunk_e = datetime.fromisoformat("2005-12-28T13:13:29"), datetime.fromisoformat("2007-12-30T13:13:29")
		self.assertNotEqual(LogParser._range_intersects_chunk(start, end, chunk_s, chunk_e), True)

	def test_raise_error_when_start_date_greater_than_end_date(self):
		start, end = datetime.fromisoformat("2021-01-01T13:13:29"), datetime.fromisoformat("2020-02-03T03:32:09")
		parser = LogParser(self.source_file)
		self.assertRaises(ValueError, parser.process_range, start, end, self.dump_file_mp)

	def tearDown(self):
		try:
			os.remove(self.dump_file_mp)
			os.remove(self.dump_file_seq)
		except FileNotFoundError:
			pass
