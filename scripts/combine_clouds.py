"""
CloudCost Sentinel - Multi-Cloud Data Combiner
Combines AWS, Azure, and GCP cost data into a single unified CSV
"""

import pandas as pd
import glob
import os
from datetime import datetime
import logging
import sys

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging."""
    os.makedirs('logs', exist_ok=True)
    
    log_filename = f"logs/combine_clouds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('CloudCombiner')
    logger.info("="*80)
    logger.info("Multi-Cloud Data Combiner Started")
    logger.info("="*80)
    
    return logger


# ============================================================================
# File Discovery
# ============================================================================

def find_latest_files(data_dir: str = 'data', logger: logging.Logger = None) -> dict:
    """
    Find the most recent CSV files for each cloud provider.
    
    Returns:
        dict with keys 'aws', 'azure', 'gcp' pointing to file paths
    """
    logger = logger or logging.getLogger(__name__)
    
    logger.info(f"Searching for cloud CSV files in: {data_dir}")
    
    files = {
        'aws': None,
        'azure': None,
        'gcp': None
    }
    
    # Find AWS files
    aws_files = glob.glob(f"{data_dir}/aws_costs_with_waste_*.csv")
    if aws_files:
        files['aws'] = max(aws_files, key=os.path.getctime)
        logger.info(f"  AWS: {files['aws']}")
    else:
        logger.warning("  AWS: No files found")
    
    # Find Azure files
    azure_files = glob.glob(f"{data_dir}/azure_costs_with_waste_*.csv")
    if azure_files:
        files['azure'] = max(azure_files, key=os.path.getctime)
        logger.info(f"  Azure: {files['azure']}")
    else:
        logger.warning("  Azure: No files found")
    
    # Find GCP files
    gcp_files = glob.glob(f"{data_dir}/gcp_costs_with_waste_*.csv")
    if gcp_files:
        files['gcp'] = max(gcp_files, key=os.path.getctime)
        logger.info(f"  GCP: {files['gcp']}")
    else:
        logger.warning("  GCP: No files found")
    
    return files


# ============================================================================
# Data Loading and Normalization
# ============================================================================

def load_and_normalize_csv(filepath: str, cloud_name: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Load a CSV file and ensure it has the 'Cloud' column.
    
    Args:
        filepath: Path to CSV file
        cloud_name: Name of cloud provider (AWS, Azure, GCP)
        logger: Logger instance
    
    Returns:
        DataFrame with Cloud column added
    """
    try:
        logger.info(f"Loading {cloud_name} data from {filepath}...")
        
        df = pd.read_csv(filepath)
        
        # Add Cloud column if not present
        if 'Cloud' not in df.columns:
            df['Cloud'] = cloud_name
        
        logger.info(f"  ✓ Loaded {len(df)} records from {cloud_name}")
        logger.info(f"    Total Cost: ${df['Cost'].sum():.2f}")
        logger.info(f"    Waste: ${df['Monthly_Waste'].sum():.2f}")
        
        return df
        
    except Exception as e:
        logger.error(f"  ✗ Failed to load {cloud_name} data: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Data Combination and Validation
# ============================================================================

def combine_dataframes(dataframes: list, logger: logging.Logger) -> pd.DataFrame:
    """
    Combine multiple cloud DataFrames into a single unified dataset.
    
    Args:
        dataframes: List of DataFrames to combine
        logger: Logger instance
    
    Returns:
        Combined DataFrame
    """
    try:
        logger.info("Combining cloud datasets...")
        
        # Filter out empty dataframes
        valid_dfs = [df for df in dataframes if not df.empty]
        
        if not valid_dfs:
            logger.error("No valid dataframes to combine!")
            return pd.DataFrame()
        
        # Combine all dataframes
        combined_df = pd.concat(valid_dfs, ignore_index=True)
        
        logger.info(f"  ✓ Combined {len(valid_dfs)} cloud datasets")
        logger.info(f"    Total records: {len(combined_df)}")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Failed to combine dataframes: {str(e)}")
        return pd.DataFrame()


def validate_combined_data(df: pd.DataFrame, logger: logging.Logger) -> bool:
    """
    Validate the combined dataset has all required columns.
    
    Returns:
        True if valid, False otherwise
    """
    required_columns = [
        'Date', 'Service', 'Region', 'Cost', 'Usage', 'Currency', 'Cloud',
        'Environment', 'Owner', 'CostCenter', 'ResourceID',
        'CPU_Utilization', 'Memory_Utilization',
        'Is_Idle', 'Is_Oversized', 'Is_Unused', 'Idle_Days',
        'Waste_Score', 'Waste_Category', 'Monthly_Waste'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
        logger.warning("Some visualizations may not work correctly")
        return False
    
    logger.info("  ✓ All required columns present")
    return True


# ============================================================================
# Summary Statistics
# ============================================================================

def print_combined_summary(df: pd.DataFrame, logger: logging.Logger) -> None:
    """Print comprehensive summary of combined cloud data."""
    
    logger.info("="*80)
    logger.info("MULTI-CLOUD DATA SUMMARY")
    logger.info("="*80)
    
    # Overall statistics
    logger.info(f"Total Records: {len(df)}")
    logger.info(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    logger.info(f"Total Cost (All Clouds): ${df['Cost'].sum():.2f}")
    logger.info(f"Total Monthly Waste: ${df['Monthly_Waste'].sum():.2f}")
    
    waste_percentage = (df['Monthly_Waste'].sum() / (df['Cost'].sum() * 30)) * 100
    logger.info(f"Waste Percentage: {waste_percentage:.1f}%")
    logger.info("")
    
    # Cost by Cloud
    logger.info("Cost by Cloud Provider:")
    cloud_costs = df.groupby('Cloud')['Cost'].sum().sort_values(ascending=False)
    for cloud, cost in cloud_costs.items():
        percentage = (cost / df['Cost'].sum()) * 100
        waste = df[df['Cloud'] == cloud]['Monthly_Waste'].sum()
        logger.info(f"  {cloud:10s}: ${cost:12.2f} ({percentage:5.1f}%)  |  Waste: ${waste:10.2f}")
    logger.info("")
    
    # Cost by Service (Top 10)
    logger.info("Cost by Service (Top 10):")
    service_costs = df.groupby('Service')['Cost'].sum().sort_values(ascending=False).head(10)
    for service, cost in service_costs.items():
        percentage = (cost / df['Cost'].sum()) * 100
        logger.info(f"  {service:20s}: ${cost:10.2f} ({percentage:5.1f}%)")
    logger.info("")
    
    # Waste Statistics
    logger.info("Waste Statistics:")
    logger.info(f"  Idle Resources: {df['Is_Idle'].sum()}")
    logger.info(f"  Oversized Resources: {df['Is_Oversized'].sum()}")
    logger.info(f"  Unused Resources: {df['Is_Unused'].sum()}")
    logger.info(f"  Total Wasteful Resources: {(df['Is_Idle'] | df['Is_Oversized'] | df['Is_Unused']).sum()}")
    logger.info("")
    
    # Waste by Category
    logger.info("Waste by Category:")
    waste_by_category = df.groupby('Waste_Category')['Monthly_Waste'].sum().sort_values(ascending=False)
    for category, waste in waste_by_category.items():
        count = df[df['Waste_Category'] == category].shape[0]
        logger.info(f"  {category:15s}: ${waste:10.2f}  ({count} resources)")
    logger.info("")
    
    # Top 10 Wasteful Resources
    logger.info("Top 10 Wasteful Resources:")
    top_waste = df.nlargest(10, 'Monthly_Waste')[['Cloud', 'Service', 'ResourceID', 'Monthly_Waste', 'Waste_Category']]
    for idx, row in top_waste.iterrows():
        logger.info(f"  {row['Cloud']:6s} | {row['Service']:15s} | {row['ResourceID']:20s} | ${row['Monthly_Waste']:8.2f} | {row['Waste_Category']}")
    
    logger.info("="*80)


# ============================================================================
# Output
# ============================================================================

def save_combined_output(df: pd.DataFrame, output_dir: str = 'data', logger: logging.Logger = None) -> str:
    """
    Save combined DataFrame to CSV.
    
    Returns:
        Path to saved file
    """
    logger = logger or logging.getLogger(__name__)
    
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/multi_cloud_costs_with_waste_{timestamp}.csv"
    
    df.to_csv(filename, index=False)
    
    logger.info(f"Combined data saved to: {filename}")
    logger.info(f"File size: {os.path.getsize(filename) / 1024 / 1024:.2f} MB")
    
    return filename


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    
    logger = setup_logging()
    
    try:
        logger.info("Starting multi-cloud data combination...")
        logger.info("")
        
        # Step 1: Find latest files
        logger.info("Step 1: Discovering cloud data files")
        files = find_latest_files(logger=logger)
        logger.info("")
        
        # Check if any files were found
        found_files = [f for f in files.values() if f is not None]
        if not found_files:
            logger.error("No cloud data files found!")
            logger.error("Please run collect_aws_costs.py, collect_azure_costs.py, or collect_gcp_costs.py first")
            return 1
        
        # Step 2: Load each cloud's data
        logger.info("Step 2: Loading cloud datasets")
        dataframes = []
        
        if files['aws']:
            aws_df = load_and_normalize_csv(files['aws'], 'AWS', logger)
            if not aws_df.empty:
                dataframes.append(aws_df)
        
        if files['azure']:
            azure_df = load_and_normalize_csv(files['azure'], 'Azure', logger)
            if not azure_df.empty:
                dataframes.append(azure_df)
        
        if files['gcp']:
            gcp_df = load_and_normalize_csv(files['gcp'], 'GCP', logger)
            if not gcp_df.empty:
                dataframes.append(gcp_df)
        
        logger.info("")
        
        # Step 3: Combine datasets
        logger.info("Step 3: Combining datasets")
        combined_df = combine_dataframes(dataframes, logger)
        
        if combined_df.empty:
            logger.error("Failed to create combined dataset")
            return 1
        
        logger.info("")
        
        # Step 4: Validate combined data
        logger.info("Step 4: Validating combined data")
        validate_combined_data(combined_df, logger)
        logger.info("")
        
        # Step 5: Save output
        logger.info("Step 5: Saving combined dataset")
        output_file = save_combined_output(combined_df, logger=logger)
        logger.info("")
        
        # Step 6: Print summary
        print_combined_summary(combined_df, logger)
        
        logger.info("")
        logger.info("="*80)
        logger.info("MULTI-CLOUD DATA COMBINATION COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info("")
        logger.info(f"Output file: {output_file}")
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Upload this CSV to Tableau Cloud")
        logger.info("  2. Create your unified multi-cloud dashboard")
        logger.info("  3. Use Extensions API for action buttons")
        logger.info("")
        
        return 0
        
    except Exception as e:
        logger.error("="*80)
        logger.error("COMBINATION FAILED")
        logger.error("="*80)
        logger.error(f"Error: {str(e)}")
        logger.error("")
        return 1


if __name__ == "__main__":
    print("Starting CloudCost Sentinel Multi-Cloud Combiner...")
    print("-" * 80)
    exit_code = main()
    print("-" * 80)
    print(f"Script completed with exit code: {exit_code}")
    sys.exit(exit_code)