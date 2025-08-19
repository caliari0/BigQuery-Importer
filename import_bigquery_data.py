import pandas as pd
from google.cloud import bigquery
import os


def import_bigquery_data():
    """
    Import data from BigQuery table basedosdados.br_ibge_censo_2022.cadastro_enderecos
    and extract CEP, latitude, and longitude columns.
    """

    # Initialize BigQuery client
    # Note: You need to set up authentication. See README for details.
    client = bigquery.Client()

    # SQL query to extract the required columns
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
        # Execute the query
        print("Executing BigQuery query...")
        df = client.query(query).to_dataframe()

        print(f"Successfully imported {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        print("\nFirst few rows:")
        print(df.head())

        # Save to CSV file
        output_file = "bigquery_data.csv"
        df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    # Check if authentication is set up
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Warning: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        print("Please set up authentication as described in the README.")

    # Import the data
    data = import_bigquery_data()
