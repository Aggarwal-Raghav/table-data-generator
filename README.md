# Table creation from SQL for hive

```
python main.py -t external -n main_tbl -c 10 -p 1 -f orc -e iceberg -r 10
```

-t -> table type\
-n -> table name\
-c -> number of columns\
-p -> number of partition columns\
-e -> Stored By\
-f -> Stored As\
-r -> rows
