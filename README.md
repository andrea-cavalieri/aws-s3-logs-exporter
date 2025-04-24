# TSV9 Log Parser

A Python utility to process and combine CSV log files from AWS S3 storage. This tool handles special formatting in log files and combines them into a single CSV file for further analysis.

## Features

- Reads CSV files from specified S3 bucket
- Cleans payload data between `;{` and `};` markers
- Handles consistent headers across multiple files
- Combines all processed files into a single CSV output
- Preserves source file information in the output

## Prerequisites

- Python 3.x
- AWS credentials configured
- Required Python packages:
  ```bash
  pip install -r requirements.txt
  ```

## Configuration

The tool uses AWS credentials. Make sure you have:
- AWS credentials configured in `~/.aws/credentials`
- Appropriate permissions to access the S3 bucket

## Usage

```bash
python _main.py
```

The script will:
1. Connect to the specified S3 bucket
2. Process all CSV files in the `log/` prefix
3. Clean special formatted content
4. Combine all files into a single CSV
5. Save the output as `combined_logs_YYYYMMDD_HHMMSS.csv`

## Output

The combined CSV file will contain:
- All columns from the original files
- Additional `source_file` column indicating the origin file
- Consistent headers across all processed data
- Semicolon (;) separated values

## Error Handling

- Files with mismatched columns are skipped
- Processing errors are logged but don't stop execution
- Empty results are handled gracefully

## Project Structure

```
tsv9/logs/
├── _main.py           # Main processing script
├── requirements.txt   # Python dependencies
└── README.md         # This documentation
```

## License

This project is proprietary and confidential.

## Author

Antonio Cavalieri