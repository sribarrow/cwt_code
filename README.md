### Package
transforms/

### data
<h6> When the code is run, data files are created here

+ Response to Questions
  + csv/dates/ (partitioned by dates)
  + csv/Question2/
  + csv/Question3/
  + csv/Question4/
  + dest_trips/
+ Bonus Question
  + csv/Bonus{n}/

### log
<h6> When the code is run, events are logged into app.log

### tests
<h6> 6 test cases to test generic functionality

### error in data found
1. origin and destination can be same (ignored)
2. booking data > departure date for exchange/cancellation (ignored)

### requires install (python v3.9 used)
python -m pip install requirements.txt

### code location
git remote add origin https://github.com/sribarrow/cwt_code.git

### Run python script (from root)
python transforms/pyspark_transform.py

# Run test (from root)
python -m unittest discover
