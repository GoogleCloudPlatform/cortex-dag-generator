# Add the test case

- Make a folder (start the name with the word `test`. e.g., test_1_1, test_1_2)
- Prepare CDC changes (input_data.csv) and create schema.json that define the schema for input_data.csv
- Create a file(keys.txt) which ccontains keys in input_data as comma seperated values
- Create the expected_output.json(For target table)
- Create the expected_view.json(For view)

# Run Test cases

- Change values for TEST_PROJECT_ID, SOURCE_DATASET and TARGET_DATASET and run the following commands
  
```
source tests/pytest.properties
pytest -s
```

If the test cases passes, all the table and output files are deleted. If a test case fails, the corresponding files and table and not deleted. The file and tables will help to investigate why the test case failed. 

On the next run, those files and table will be automatically deleted. Hence you dont need to delete after investigation.

To run only integration tests:

```
pytest -m integration
```

To run pytest coverage
```
pytest --cov src tests --cov-fail-under 8
```

To generate coverage report for coverage gutters:
```
pytest --cov src tests --cov-fail-under 80 --cov-report xml:cov.xml
```

