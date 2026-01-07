from google.cloud import bigquery
import os

def simple_bigquery_test():
    """Simple test to access BigQuery public dataset."""
    
    try:
        print("Testing BigQuery access...")
        
        # Initialize client without specifying project
        client = bigquery.Client()
        
        # Try to access the public dataset
        query = """
        SELECT 
            cep,
            latitude,
            longitude
        FROM `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
        WHERE cep IS NOT NULL 
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
        LIMIT 3
        """
        
        print("Executing query...")
        df = client.query(query).to_dataframe()
        
        print(f" Success! Retrieved {len(df)} rows")
        print(f"Columns: {list(df.columns)}")
        print("\nData:")
        print(df)
        
        return True
        
    except Exception as e:
        print(f" Error: {e}")
        print(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    print("Simple BigQuery test...")
    simple_bigquery_test()
