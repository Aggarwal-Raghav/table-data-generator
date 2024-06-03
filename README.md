# Table creation from SQL for hive

```
python main.py -t EXTERNAL -c 50 -p 5 -f orc -e iceberg
```

-t -> table type
-c -> number of columns
-p -> number of partition columns
-f -> Stored As
-e -> Stored By
