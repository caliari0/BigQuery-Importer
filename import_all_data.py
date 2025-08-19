import pandas as pd
from google.cloud import bigquery
import os
import time

def import_all_bigquery_data():
    """
    Import ALL data from BigQuery table basedosdados.br_ibge_censo_2022.cadastro_enderecos
    and extract CEP, latitude, and longitude columns.
    """
    
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # SQL query to extract ALL the required columns (no LIMIT)
    query = """
    SELECT 
        cep,
        latitude,
        longitude
    FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
    WHERE cep IS NOT NULL 
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
    """
    
    try:
        print("🚀 Starting BigQuery import of ALL data...")
        print("This may take a while depending on the dataset size...")
        print("-" * 50)
        
        # First, let's check how many rows we're dealing with
        count_query = """
        SELECT COUNT(*) as total_rows
        FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
        WHERE cep IS NOT NULL 
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
        """
        
        print("📊 Counting total rows...")
        count_result = client.query(count_query).to_dataframe()
        total_rows = count_result.iloc[0]['total_rows']
        print(f"📈 Total rows to import: {total_rows:,}")
        
        if total_rows > 1000000:  # If more than 1 million rows
            print("⚠️  Large dataset detected! This may take several minutes...")
            print("💡 Consider using batch processing for very large datasets")
        
        print("\n🔄 Executing main query...")
        start_time = time.time()
        
        # Execute the main query
        df = client.query(query).to_dataframe()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Successfully imported {len(df):,} rows in {duration:.2f} seconds")
        print(f"📊 Columns: {list(df.columns)}")
        print(f"💾 Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        print("\n📋 Sample data (first 5 rows):")
        print(df.head())
        
        print("\n📋 Sample data (last 5 rows):")
        print(df.tail())
        
        # Save to CSV file
        output_file = "bigquery_all_data.csv"
        print(f"\n💾 Saving data to {output_file}...")
        df.to_csv(output_file, index=False)
        print(f"✅ Data saved to {output_file}")
        
        # Also save a sample for quick viewing
        sample_file = "bigquery_sample_data.csv"
        df.head(1000).to_csv(sample_file, index=False)
        print(f"📄 Sample data (first 1000 rows) saved to {sample_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"Error type: {type(e).__name__}")
        return None

if __name__ == "__main__":
    print("=" * 60)
    print("🌐 BigQuery Complete Data Import")
    print("=" * 60)
    
    # Check if authentication is set up
    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        print("⚠️  Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        print("   Using gcloud authentication instead...")
    
    # Import all the data
    data = import_all_bigquery_data()
    
    if data is not None:
        print("\n🎉 Import completed successfully!")
        print(f"📊 Total rows imported: {len(data):,}")
        print(f"📁 Files created:")
        print(f"   - bigquery_all_data.csv (complete dataset)")
        print(f"   - bigquery_sample_data.csv (first 1000 rows)")
    else:
        print("\n❌ Import failed. Please check the error messages above.")
