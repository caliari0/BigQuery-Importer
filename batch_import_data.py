import pandas as pd
from google.cloud import bigquery
import os
import time
from datetime import datetime

# Global configuration
OUTPUT_DIR = "batch_data"


def batch_import_bigquery_data(batch_size=100000):
    """
    Import ALL data from BigQuery table using batch processing.
    This approach is much more memory-efficient for large datasets.

    Args:
        batch_size (int): Number of rows to process in each batch
    """

    # Initialize BigQuery client
    client = bigquery.Client()

    # First, get the total count
    count_query = """
    SELECT COUNT(*) as total_rows
    FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
    WHERE cep IS NOT NULL 
      AND latitude IS NOT NULL 
      AND longitude IS NOT NULL
    """

    print("📊 Counting total rows...")
    count_result = client.query(count_query).to_dataframe()
    total_rows = count_result.iloc[0]["total_rows"]
    print(f"📈 Total rows to import: {total_rows:,}")

    # Calculate number of batches
    num_batches = (total_rows + batch_size - 1) // batch_size
    print(f"🔄 Will process in {num_batches} batches of {batch_size:,} rows each")

    # Create output directory for batches
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📁 Created output directory: {OUTPUT_DIR}")

    # Process data in batches
    all_data = []
    start_time = time.time()

    for batch_num in range(num_batches):
        batch_start_time = time.time()
        offset = batch_num * batch_size

        print(
            f"\n🔄 Processing batch {batch_num + 1}/{num_batches} (rows {offset:,} to {min(offset + batch_size, total_rows):,})"
        )

        # Query for this batch
        batch_query = f"""
        SELECT 
            cep,
            latitude,
            longitude
        FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
        WHERE cep IS NOT NULL 
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
        ORDER BY cep, latitude, longitude
        LIMIT {batch_size} OFFSET {offset}
        """

        try:
            # Execute batch query
            df_batch = client.query(batch_query).to_dataframe()

            if len(df_batch) == 0:
                print(f"⚠️  Batch {batch_num + 1} returned 0 rows - stopping")
                break

            # Save batch to individual file
            batch_filename = (
                f"{OUTPUT_DIR}/batch_{batch_num + 1:04d}_{len(df_batch):,}_rows.csv"
            )
            df_batch.to_csv(batch_filename, index=False)

            # Calculate progress
            processed_rows = offset + len(df_batch)
            progress = (processed_rows / total_rows) * 100
            batch_duration = time.time() - batch_start_time

            print(f"✅ Batch {batch_num + 1} completed:")
            print(f"   📊 Rows: {len(df_batch):,}")
            print(f"   ⏱️  Time: {batch_duration:.2f}s")
            print(f"   📁 Saved: {batch_filename}")
            print(
                f"   📈 Progress: {progress:.1f}% ({processed_rows:,}/{total_rows:,})"
            )

            # Optional: Keep in memory for final combined file
            # Comment out the next line if you only want individual batch files
            all_data.append(df_batch)

        except Exception as e:
            print(f"❌ Error in batch {batch_num + 1}: {e}")
            continue

    # Calculate total time
    total_duration = time.time() - start_time

    print(f"\n🎉 Batch processing completed!")
    print(f"⏱️  Total time: {total_duration:.2f} seconds")
    print(f"📁 Batch files saved in: {OUTPUT_DIR}/")

    # Create combined file if requested
    if all_data:
        print(f"\n🔄 Creating combined file...")
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_filename = "bigquery_all_data_combined.csv"
        combined_df.to_csv(combined_filename, index=False)
        print(f"✅ Combined file saved: {combined_filename}")
        print(f"📊 Total rows in combined file: {len(combined_df):,}")

        # Memory cleanup
        del all_data
        del combined_df

    return True


def create_sample_from_batches(sample_size=10000):
    """
    Create a sample file from the first few batches for quick viewing.
    """
    if not os.path.exists(OUTPUT_DIR):
        print("❌ No batch data found. Run batch import first.")
        return

    print(f"\n📄 Creating sample file from batches...")

    sample_data = []
    batch_files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")])

    for batch_file in batch_files[:5]:  # First 5 batches
        file_path = os.path.join(OUTPUT_DIR, batch_file)
        df = pd.read_csv(file_path)
        sample_data.append(df)

        if len(pd.concat(sample_data, ignore_index=True)) >= sample_size:
            break

    if sample_data:
        sample_df = pd.concat(sample_data, ignore_index=True).head(sample_size)
        sample_filename = "bigquery_sample_from_batches.csv"
        sample_df.to_csv(sample_filename, index=False)
        print(f"✅ Sample file created: {sample_filename}")
        print(f"📊 Sample rows: {len(sample_df):,}")
    else:
        print("❌ No data found in batches")


if __name__ == "__main__":
    print("=" * 70)
    print("🌐 BigQuery Batch Data Import")
    print("=" * 70)

    # Check if authentication is set up
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print(
            "⚠️  Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set."
        )
        print("   Using gcloud authentication instead...")

    # Get user preference for batch size
    print("\n📋 Batch Processing Options:")
    print("1. Small batches (50,000 rows) - More memory efficient, slower")
    print("2. Medium batches (100,000 rows) - Balanced approach")
    print("3. Large batches (250,000 rows) - Faster, more memory usage")

    try:
        choice = input(
            "\nSelect batch size (1-3) or press Enter for default (100,000): "
        ).strip()

        if choice == "1":
            batch_size = 50000
        elif choice == "2":
            batch_size = 100000
        elif choice == "3":
            batch_size = 250000
        else:
            batch_size = 100000
            print(f"Using default batch size: {batch_size:,}")

        print(f"\n🚀 Starting batch import with batch size: {batch_size:,}")

        # Start batch processing
        success = batch_import_bigquery_data(batch_size)

        if success:
            # Create sample file
            create_sample_from_batches()

            print(f"\n🎉 All done! Check the '{OUTPUT_DIR}/' folder for batch files.")
            print(f"📁 Files created:")
            print(f"   - Individual batch files in {OUTPUT_DIR}/")
            print(f"   - Combined file: bigquery_all_data_combined.csv")
            print(f"   - Sample file: bigquery_sample_from_batches.csv")

    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        print("Partial batch files may have been created in the batch_data/ folder")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
