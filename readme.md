# Log Parser

#### Overview
- Log parsing library that utilizes multiple cores of the system to parse the file in parallel.
- Consumes costant memory, independed of the file size.
- The file is broken into chunks of size no more than 750KB. That means, for an octa core processor,
  no more than 6 MB of memory will be consumed at any time.
- Uses divide and conquer strategy to gather the records. For each chunk, if there is any intersection between
  the provided range and the chunks date range, then only the chunk is processed, skipped altogether otherwise.
- Since all of this is done is parallel, the process of filtering the records becomes very fast.
- To run the program, please refer to [running the program](#running-the-Program).
- Maintains internal cache to process subsequent queries faster.


#### Performance
- Tested on file of size ~ 232 MB.
- Parallel processing made the program run 5-7 times faster than the sequential counterpart.
- The difference is not that evident for smaller files, but as the files get bigger (> 30 MB), the difference gets more pronounced.
- The file that it was tested on had 3 million records and `Records Filtered` column tells how many records fell within the range.

|Total Records       |Records Filtered  | exec time parallel | exec time sequential |
|--------------------|------------------|--------------------|----------------------|
|3000000             |0                 |0.8046s             |3.2461s               |
|3000000             |895161            |7.8014s             |39.0130s              | 
|3000000             |2367315           |3.1274s             |16.5686s              |


#### Answering multiple queries
The Parser can also be used as a library which use you can integrate with your client code to answer multiple queries. 
Here is a demo on how to do it:
```python
from parser import *


def main():
    source = "source_file.txt"
    dump_to = "some_dump_file.json"
    start = datetime.fromisoformat("some valid date string")
    end = datetime.fromisoformat("some valid date string")
    
    # the constructor takes source file as the argument
    log_parser = LogParser(source)
    
    # start answering queries and dump the filtered records into the destination file
    count, t = log_parser.process_range(start, end, dump_to)


if __name__ == '__main__':
    main()
```


- If you have a list of queries of the form `[[start_date_a, end_date_a], [start_date_b, end_date_b], ...]`, 
then for each `(start_date, end_date)` query, you can spawn a new process and all the queries will be
answered in parallel.
- The chunk processor while processing the chunk, stores the metadata of the chunk in an internal process safe cache.
Once the cache is populated, the decision of processing a chunk can be made merely by a cache lookup; without having to 
process the chunk multiple times.


#### Running the Program
```sh
$ python3 parser.py file_name from_date to_date
```

- Required Python >= 3.7
- Move inside the directoy where `parser.py` file is, and run the above command
- The program takes 3 parameters, path to the source file, start date and end date
- The format of the dates should be: `YYYY-MM-DDThh:mm:ssZ`
- Running this command will dump all the filtered records which fall within the specified range into `records.json` file


#### Running the Unit Tests
```sh
$ python3 -m unittest
```

* Move inside the directoy where `test_parser.py` file is, and run the above command
* I have included a `sample1.txt` file in the directory for the test cases to pass.
* If you want to run the unit tests for a new text file, you will need to modify `test_parser.py`:
    -  Inside the `setUp` method, set the `self.source_file` to the new source file name.
    -  Inside the `test_count_is_greater_than_zero` method, change `start` and `end` dates to the dates you want to test.
* Other test cases are independent of the source file, so they should be able to run regardless. 