# BigQuery Importer

A comprehensive and **highly flexible** Python application for importing data from Google BigQuery. While the current implementation focuses on geographic data from the IBGE Census 2022 dataset, the application is designed to be **easily adaptable** for any BigQuery query, table, or dataset with minimal code changes.

## 🎯 What This App Does

This application connects to Google BigQuery and imports data from any table or dataset. The current implementation demonstrates usage with the `basedosdados.br_ibge_censo_2022.cadastro_enderecos` table, extracting:
- **CEP** (Brazilian postal codes)
- **Latitude** coordinates
- **Longitude** coordinates

**🔧 Generic & Adaptable**: The app is designed to work with **any BigQuery query or dataset**. You can easily modify the SQL queries, column selections, and data processing logic to work with:
- **Any BigQuery table** (public or private)
- **Any columns** (not just geographic data)
- **Any filtering conditions** (WHERE clauses)
- **Any data transformations** (aggregations, joins, etc.)
- **Any output format** (CSV, JSON, database exports, etc.)

### Current Use Case Examples:
- Geographic analysis and mapping
- Address validation and geocoding
- Location-based services
- Research and data analysis
- Business intelligence applications

### Potential Adaptations:
- **E-commerce data**: Customer analytics, sales reports, inventory data
- **Financial data**: Transaction logs, market data, risk analysis
- **Healthcare data**: Patient records, medical statistics, research data
- **Logistics data**: Shipping information, route optimization, delivery tracking
- **Social media data**: User engagement, content analysis, trend data

## 🏗️ Application Structure

### Core Components

#### 1. **`import_bigquery_data.py`** - Basic Data Import
- **Purpose**: Simple, single-run import of BigQuery data
- **Features**: 
  - Extracts CEP, latitude, and longitude data
  - Filters out null values
  - Saves results to `bigquery_data.csv`
  - Basic error handling and progress reporting
- **Best for**: Small datasets, testing, or one-time imports

#### 2. **`import_all_data.py`** - Complete Dataset Import
- **Purpose**: Import the entire dataset with enhanced features
- **Features**:
  - Counts total rows before import
  - Progress tracking and timing
  - Memory usage monitoring
  - Creates both complete and sample datasets
  - Enhanced error handling and user feedback
- **Best for**: Medium-sized datasets when you need all the data

#### 3. **`batch_import_data.py`** - Memory-Efficient Batch Processing
- **Purpose**: Process large datasets in manageable chunks
- **Features**:
  - Configurable batch sizes (50K, 100K, or 250K rows)
  - Individual batch file creation
  - Progress tracking per batch
  - Memory-efficient processing
  - Option to create combined output file
  - Sample file generation from batches
- **Best for**: Large datasets, memory-constrained environments, or when you need to process data incrementally

#### 4. **`test_connection.py`** - Connection Testing
- **Purpose**: Verify BigQuery connectivity and basic functionality
- **Features**:
  - Tests authentication and API access
  - Runs a small sample query
  - Provides troubleshooting tips
- **Best for**: Initial setup verification and debugging

#### 5. **`simple_test.py`** - Minimal Connection Test
- **Purpose**: Basic connectivity test with minimal dependencies
- **Features**:
  - Simple connection test
  - Minimal data retrieval (3 rows)
  - Basic error reporting
- **Best for**: Quick connectivity checks

### Supporting Files

- **`requirements.txt`**: Python dependencies (Google Cloud BigQuery and Pandas)
- **`batch_data/`**: Directory for storing batch processing output files
- **`bigquery_data.csv`**: Output file from basic import
- **`bigquery_all_data.csv`**: Complete dataset output
- **`bigquery_sample_data.csv`**: Sample dataset for quick viewing

## 🚀 How It Works

### 1. **Authentication Setup**
The application uses Google Cloud authentication. You can set it up in two ways:
- Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your service account key file
- Use `gcloud auth application-default login` for local development

### 2. **Data Extraction Process**
1. **Connection**: Establishes connection to Google BigQuery
2. **Query Execution**: Runs SQL queries to extract data (easily customizable)
3. **Data Processing**: Filters and processes the results (adaptable logic)
4. **Output Generation**: Saves data to files with progress reporting

### 3. **Batch Processing Logic** (for large datasets)
1. **Count Total Rows**: Determines dataset size
2. **Calculate Batches**: Divides data into manageable chunks
3. **Process Incrementally**: Handles each batch separately
4. **File Management**: Creates individual batch files and optional combined output

### 4. **Adaptability Features**
- **Modular Design**: Each component can be modified independently
- **Configurable Queries**: SQL statements are easily changeable
- **Flexible Output**: Data processing and export logic is customizable
- **Reusable Framework**: Core functionality works with any dataset structure

## 📊 Data Source

### Current Implementation
The application currently demonstrates usage with the **Base dos Dados** public dataset:
- **Table**: `basedosdados.br_ibge_censo_2022.cadastro_enderecos`
- **Source**: Brazilian Institute of Geography and Statistics (IBGE) Census 2022
- **Content**: Address registration data with geographic coordinates
- **Coverage**: Brazil-wide postal code and location information

### 🔧 Adapting to Other Data Sources
The app is **not limited** to this specific dataset. You can easily modify it to work with:
- **Any Google Cloud BigQuery table** (public or private)
- **Any dataset structure** (different columns, data types, etc.)
- **Any query complexity** (simple SELECTs to complex JOINs and aggregations)
- **Any data source** (Google Analytics, Firebase, custom datasets, etc.)

**Example**: To use with a different table, simply change the SQL query in any of the import scripts:
```python
# Change this line in any import script:
query = """
SELECT 
    your_column1,
    your_column2,
    your_column3
FROM `your_project.your_dataset.your_table`
WHERE your_condition = 'your_value'
"""
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.7+
- Google Cloud account with BigQuery API enabled
- Appropriate permissions to access public datasets

### Installation
```bash
# Clone or download the application
cd bigquery-importer

# Install dependencies
pip install -r requirements.txt

# Set up authentication
gcloud auth application-default login
```

### Environment Variables (Optional)
```bash
# For service account authentication
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

## 🎮 Usage Examples

### Quick Start - Test Connection
```bash
python test_connection.py
```

### Basic Data Import
```bash
python import_bigquery_data.py
```

### Complete Dataset Import
```bash
python import_all_data.py
```

### Batch Processing (Recommended for Large Datasets)
```bash
python batch_import_data.py
```

## 📈 Performance Considerations

### **Small Datasets** (< 100K rows)
- Use `import_bigquery_data.py` or `import_all_data.py`
- Fast processing, minimal memory usage

### **Medium Datasets** (100K - 1M rows)
- Use `import_all_data.py` for complete import
- Monitor memory usage during processing

### **Large Datasets** (> 1M rows)
- **Recommended**: Use `batch_import_data.py`
- Configurable batch sizes for memory optimization
- Progress tracking and resume capability
- Individual batch files for incremental processing

## 🔧 Customization Options

### **Easy Adaptations for Different Use Cases**

#### 1. **Change Data Source**
```python
# In any import script, modify the query:
query = """
SELECT 
    customer_id,           # Instead of cep
    purchase_amount,       # Instead of latitude  
    transaction_date       # Instead of longitude
FROM `your_project.analytics.customer_transactions`
WHERE transaction_date >= '2024-01-01'
"""
```

#### 2. **Modify Data Processing**
```python
# Change column names and data handling:
df = client.query(query).to_dataframe()
# Process your specific columns instead of geographic data
df['formatted_amount'] = df['purchase_amount'].apply(lambda x: f"${x:.2f}")
```

#### 3. **Customize Output Files**
```python
# Change output filenames to match your data:
output_file = "customer_transactions.csv"  # Instead of bigquery_data.csv
```

### **Batch Size Configuration**
In `batch_import_data.py`, you can choose:
- **Small batches**: 50,000 rows (memory efficient, slower)
- **Medium batches**: 100,000 rows (balanced approach)
- **Large batches**: 250,000 rows (faster, more memory usage)

### **Output File Naming**
All scripts create timestamped or descriptive filenames:
- `bigquery_data.csv` - Basic import output
- `bigquery_all_data.csv` - Complete dataset
- `batch_XXXX_XX_rows.csv` - Individual batch files
- `bigquery_sample_data.csv` - Sample datasets

**💡 Tip**: Rename these files to match your specific use case for better organization.

## 🚨 Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Run `gcloud auth application-default login`
   - Check service account permissions
   - Verify API is enabled in Google Cloud Console

2. **Memory Issues**
   - Use batch processing for large datasets
   - Reduce batch size in `batch_import_data.py`
   - Monitor system memory during processing

3. **Connection Timeouts**
   - Check internet connectivity
   - Verify BigQuery API status
   - Consider using smaller batch sizes

### Error Messages
- **"GOOGLE_APPLICATION_CREDENTIALS not set"**: Set up authentication
- **"BigQuery API not enabled"**: Enable BigQuery API in Google Cloud Console
- **"Permission denied"**: Check account permissions and dataset access

## 📁 Output Files

### Generated Files
- **Individual CSV files**: Each containing the specified columns
- **Sample files**: First 1000-10000 rows for quick analysis
- **Batch files**: Individual chunks for large datasets
- **Combined files**: Merged results from batch processing

### File Formats
- **Encoding**: UTF-8
- **Delimiter**: Comma (,)
- **Headers**: Included
- **Index**: Excluded (clean data)

## 🔮 Future Enhancements

### **Core Framework Improvements**
- **Data validation**: Schema validation and data quality checks
- **Incremental updates**: Delta processing for new data
- **Multiple datasets**: Support for multiple data sources simultaneously
- **API endpoints**: REST API for programmatic access
- **Data transformation**: Additional data processing options
- **Export formats**: Support for JSON, Parquet, or database exports

### **Adaptability Enhancements**
- **Template system**: Pre-built templates for common data types (analytics, logs, transactions)
- **Configuration files**: YAML/JSON configs for different data sources
- **Plugin architecture**: Modular system for custom data processors
- **Query builder**: GUI for building and testing BigQuery queries
- **Data preview**: Interactive preview of data before full import
- **Schema detection**: Automatic detection of table structure and data types

## 📚 Additional Resources

- [Google Cloud BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Base dos Dados](https://basedosdados.org/) - Brazilian open data platform
- [IBGE Census 2022](https://censo2022.ibge.gov.br/) - Official census information
- [Google Cloud Authentication](https://cloud.google.com/docs/authentication)

## 🤝 Contributing

This application is designed for **anyone working with BigQuery data**, regardless of the specific dataset or use case. The framework is intentionally generic and adaptable. Feel free to:
- Report issues or bugs
- Suggest improvements for broader compatibility
- Contribute code enhancements for different data types
- Share use cases and success stories from various domains
- Create templates for common data import patterns
- Suggest new export formats or processing options

### **Why This App is Generic**
- **Framework-based**: Core functionality is dataset-agnostic
- **Query-driven**: Works with any valid BigQuery SQL
- **Modular design**: Components can be swapped or modified independently
- **Extensible**: Easy to add new features without breaking existing functionality

## 📄 License

This project is open source and available under standard open source licenses. The current example data source (IBGE Census 2022) is public domain data provided by the Brazilian government, but the application framework itself is designed to work with any BigQuery data source.

---

## 🎯 **Key Takeaway: This is a Generic Framework**

**Don't let the current example fool you!** While this app demonstrates usage with geographic data, it's actually a **flexible, reusable framework** for any BigQuery data import needs. The core architecture handles:

✅ **Any BigQuery table or dataset**  
✅ **Any column structure or data types**  
✅ **Any query complexity**  
✅ **Any output format requirements**  
✅ **Any batch processing needs**  

**To adapt for your use case, you typically only need to change:**
1. The SQL query (table name, columns, filters)
2. Output filenames (optional)
3. Data processing logic (if needed)

Everything else - authentication, connection handling, batch processing, error handling, progress tracking - works the same way for any data source!

---

**Happy data importing! 🚀📊**
