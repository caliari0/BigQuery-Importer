from google.cloud import bigquery
import pandas as pd


def test_bigquery_connection():
    """Test BigQuery connection and basic functionality."""

    try:
        # Initialize BigQuery client
        print("Initializing BigQuery client...")
        client = bigquery.Client()

        # Test with a simple query to check connection
        test_query = """
        SELECT 
            cep,
            latitude,
            longitude
        FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
        WHERE cep IS NOT NULL 
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
        LIMIT 5
        """

        print("Testing connection with a small query...")
        df = client.query(test_query).to_dataframe()

        print(f"✅ Success! Retrieved {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        print("\nSample data:")
        print(df.head())

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTroubleshooting tips:")
        print(
            "1. Make sure you're authenticated: gcloud auth application-default login"
        )
        print("2. Check if BigQuery API is enabled in your Google Cloud project")
        print("3. Verify you have the necessary permissions")
        return False


if __name__ == "__main__":
    print("Testing BigQuery connection...")
    test_bigquery_connection()
