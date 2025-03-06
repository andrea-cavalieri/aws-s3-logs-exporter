CREATE TABLE tsv8logs
ENGINE = MergeTree
ORDER BY tuple()  AS
SELECT *
FROM file('combined_logs_20250306_093002.csv', CSV)  settings format_csv_delimiter = ';';      


SELECT * from tsv8logs INTO OUTFILE '/tmp/tsv8logs.parquet' FORMAT Parquet;


