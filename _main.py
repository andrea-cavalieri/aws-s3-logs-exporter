import boto3
import pandas as pd
from urllib.parse import urlparse
import io
import re
from datetime import datetime

AWS_PROFILE = 'AdministratorAccess-061039805219'

def clean_csv_content(csv_content):
    """
    Cleans CSV content by handling special formatting:
    1. Removes content between ;{ and };
    2. Preserves the structure ;{};
    """
    # Define your new content (it can span multiple lines)
    new_content = "fake-payload"

    content_str = csv_content.decode('utf-8')
    # Match content between ;{ and }; and replace with empty content
    ## cleaned_content = re.sub(r';{[^}]*?};', ';{};', content_str)

    # Use a non-greedy regular expression to match content between ;{ and };
    pattern = re.compile(r'(;{).*?(};)', re.DOTALL)
    # Replace the content while keeping the delimiters intact
    cleaned_content = pattern.sub(r'\1' + new_content + r'\2', content_str)

    return cleaned_content

def parse_s3_logs():
    # Initialize S3 client with specific profile
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client('s3')
    
    # Parse the S3 URL
    bucket_name = 'tsv-prod-logs'
    prefix = 'log/'

    # Create empty list to store all dataframes
    all_dfs = []
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'combined_logs_{timestamp}.csv'

    # List all CSV files in the bucket with the given prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # Process each CSV file
    first_file = True
    headers = None
    
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                try:
                    # Get the CSV file
                    response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                    
                    # Read and clean the content
                    raw_content = response['Body'].read()
                    cleaned_content = clean_csv_content(raw_content)
                    csv_buffer = io.StringIO(cleaned_content)
                    
                    if first_file:
                        # For the first file, read headers and store them
                        df = pd.read_csv(
                            csv_buffer,
                            sep=';',
                            header=0,
                            encoding='utf-8',
                            engine='python'
                        )
                        headers = df.columns.tolist()
                        first_file = False
                    else:
                        # For subsequent files, use the stored headers
                        df = pd.read_csv(
                            csv_buffer,
                            sep=';',
                            header=0,
                            names=headers,
                            encoding='utf-8',
                            engine='python'
                        )
                    
                    # Add source file information
                    df['source_file'] = obj['Key']
                    
                    # Append to our list of dataframes
                    all_dfs.append(df)

                except Exception as e:
                    print(f"Error processing {obj['Key']}: {str(e)}")

    if all_dfs:
        # Combine all dataframes
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save to single CSV file
        combined_df.to_csv(output_file, index=False, sep=';')
        print(f"All data saved to: {output_file}")
    else:
        print("No data was processed")

if __name__ == "__main__":
    parse_s3_logs()