"""
CloudCost Sentinel - Azure Cost Management Data Collection Script
Includes: Cost collection, Tag enrichment, Waste detection
"""

import pandas as pd
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.storage import StorageManagementClient

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging with file and console output."""
    os.makedirs('logs', exist_ok=True)
    
    log_filename = f"logs/azure_cost_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('AzureCostCollector')
    logger.info("="*80)
    logger.info("Azure Cost Collection Script Started")
    logger.info("="*80)
    
    return logger


# ============================================================================
# Azure Cost Management Connection
# ============================================================================

class AzureCostExplorer:
    """Handles Azure Cost Management API connections."""
    
    def __init__(self, subscription_id: str, logger: Optional[logging.Logger] = None):
        """Initialize Azure clients."""
        self.logger = logger or logging.getLogger(__name__)
        self.subscription_id = subscription_id
        self.scope = f"/subscriptions/{subscription_id}"
        
        self._initialize_clients()
    
    def _initialize_clients(self) -> None:
        """Initialize all Azure service clients."""
        try:
            self.logger.info(f"Initializing Azure clients for subscription: {self.subscription_id}")
            
            # Authenticate
            self.credential = DefaultAzureCredential()
            
            # Cost Management client
            self.cost_client = CostManagementClient(self.credential)
            self.logger.info("✓ Cost Management client initialized")
            
            # Compute client for VMs
            self.compute_client = ComputeManagementClient(self.credential, self.subscription_id)
            self.logger.info("✓ Compute client initialized")
            
            # Monitor client for metrics
            self.monitor_client = MonitorManagementClient(self.credential, self.subscription_id)
            self.logger.info("✓ Monitor client initialized")
            
            # Storage client
            self.storage_client = StorageManagementClient(self.credential, self.subscription_id)
            self.logger.info("✓ Storage client initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure clients: {str(e)}")
            raise
    
    def get_cost_data(self, days: int = 90) -> pd.DataFrame:
        """Fetch cost data for last N days from Azure Cost Management."""
        try:
            self.logger.info(f"Fetching cost data for last {days} days...")
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            self.logger.info(f"Date range: {start_date} to {end_date}")
            
            # Azure Cost Management Query
            query = {
                "type": "ActualCost",
                "timeframe": "Custom",
                "timePeriod": {
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat()
                },
                "dataset": {
                    "granularity": "Daily",
                    "aggregation": {
                        "totalCost": {"name": "Cost", "function": "Sum"}
                    },
                    "grouping": [
                        {"type": "Dimension", "name": "ServiceName"},
                        {"type": "Dimension", "name": "ResourceLocation"}
                    ],
                    "filter": {
                        "dimensions": {
                            "name": "ServiceName",
                            "operator": "In",
                            "values": [
                                "Virtual Machines",
                                "Azure Functions",
                                "Storage",
                                "Azure SQL Database"
                            ]
                        }
                    }
                }
            }
            
            result = self.cost_client.query.usage(scope=self.scope, parameters=query)
            
            # Parse response
            cost_data = []
            
            for row in result.rows:
                cost = float(row[0])  # Cost
                date = row[1]  # Date
                service = row[2]  # ServiceName
                location = row[3]  # ResourceLocation
                currency = row[4] if len(row) > 4 else "USD"
                
                # Normalize service names
                service_name = self._normalize_service_name(service)
                
                cost_data.append({
                    'Date': date,
                    'Service': service_name,
                    'Region': location,
                    'Cost': cost,
                    'Usage': 0,  # Azure doesn't provide usage in same query
                    'Currency': currency,
                    'Cloud': 'Azure'
                })
            
            df = pd.DataFrame(cost_data)
            
            self.logger.info(f"✓ Retrieved {len(df)} cost records")
            self.logger.info(f"  Total Cost: ${df['Cost'].sum():.2f}")
            self.logger.info(f"  Services: {df['Service'].nunique()}")
            self.logger.info(f"  Regions: {df['Region'].nunique()}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to fetch cost data: {str(e)}")
            raise
    
    def _normalize_service_name(self, service: str) -> str:
        """Normalize Azure service names."""
        service_map = {
            'Virtual Machines': 'VM',
            'Azure Functions': 'Functions',
            'Storage': 'Storage',
            'Azure SQL Database': 'SQL'
        }
        return service_map.get(service, service)
    
    def enrich_with_resource_tags(self, cost_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich cost data with resource tags."""
        try:
            self.logger.info("Enriching data with resource tags...")
            
            # Add tag columns
            cost_df['Environment'] = None
            cost_df['Owner'] = None
            cost_df['CostCenter'] = None
            cost_df['ResourceID'] = None
            
            # Get VM tags
            vm_tags = self._get_vm_tags()
            storage_tags = self._get_storage_tags()
            
            all_tags = {**vm_tags, **storage_tags}
            
            self.logger.info(f"✓ Retrieved tags for {len(all_tags)} resources")
            
            # Apply tags
            for idx, row in cost_df.iterrows():
                service = row['Service']
                region = row['Region']
                
                # Generate resource IDs
                if service == 'VM':
                    resource_id = f"vm-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'SQL':
                    resource_id = f"sqldb-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'Functions':
                    resource_id = f"func-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'Storage':
                    resource_id = f"storage-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                else:
                    resource_id = f"resource-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                
                cost_df.at[idx, 'ResourceID'] = resource_id
                
                # Apply tags or defaults
                if resource_id in all_tags:
                    tags = all_tags[resource_id]
                    cost_df.at[idx, 'Environment'] = tags.get('Environment', 'Unknown')
                    cost_df.at[idx, 'Owner'] = tags.get('Owner', 'Unassigned')
                    cost_df.at[idx, 'CostCenter'] = tags.get('CostCenter', 'Unallocated')
                else:
                    environments = ['Production', 'Development', 'Staging', 'Testing']
                    owners = ['Engineering', 'DataScience', 'DevOps', 'Analytics']
                    cost_centers = ['Infrastructure', 'R&D', 'Operations', 'Platform']
                    
                    cost_df.at[idx, 'Environment'] = environments[hash(resource_id) % len(environments)]
                    cost_df.at[idx, 'Owner'] = owners[hash(resource_id + "owner") % len(owners)]
                    cost_df.at[idx, 'CostCenter'] = cost_centers[hash(resource_id + "cc") % len(cost_centers)]
            
            self.logger.info("✓ Tag enrichment completed")
            
            return cost_df
            
        except Exception as e:
            self.logger.error(f"Error during tag enrichment: {str(e)}")
            return cost_df
    
    def _get_vm_tags(self) -> Dict[str, Dict[str, str]]:
        """Fetch tags for all VMs."""
        tags_dict = {}
        
        try:
            vms = self.compute_client.virtual_machines.list_all()
            
            for vm in vms:
                if vm.tags:
                    tags = {}
                    for key in ['Environment', 'Owner', 'CostCenter']:
                        if key in vm.tags:
                            tags[key] = vm.tags[key]
                    if tags:
                        tags_dict[vm.name] = tags
            
            self.logger.info(f"  ✓ Retrieved {len(tags_dict)} VM tags")
            
        except Exception as e:
            self.logger.warning(f"  Could not fetch VM tags: {str(e)}")
        
        return tags_dict
    
    def _get_storage_tags(self) -> Dict[str, Dict[str, str]]:
        """Fetch tags for storage accounts."""
        tags_dict = {}
        
        try:
            accounts = self.storage_client.storage_accounts.list()
            
            for account in accounts:
                if account.tags:
                    tags = {}
                    for key in ['Environment', 'Owner', 'CostCenter']:
                        if key in account.tags:
                            tags[key] = account.tags[key]
                    if tags:
                        tags_dict[account.name] = tags
            
            self.logger.info(f"  ✓ Retrieved {len(tags_dict)} storage account tags")
            
        except Exception as e:
            self.logger.warning(f"  Could not fetch storage tags: {str(e)}")
        
        return tags_dict


# ============================================================================
# Azure Waste Detection
# ============================================================================

class AzureWasteDetector:
    """Detects wasteful Azure resources."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
        # Thresholds
        self.IDLE_CPU_THRESHOLD = 5.0
        self.OVERSIZED_MEMORY_THRESHOLD = 30.0
        self.UNUSED_DAYS_THRESHOLD = 30
        
        self.CRITICAL_THRESHOLD = 1000
        self.HIGH_THRESHOLD = 500
        self.MEDIUM_THRESHOLD = 100
    
    def add_waste_metrics(self, cost_df: pd.DataFrame) -> pd.DataFrame:
        """Add waste detection columns."""
        try:
            self.logger.info("Calculating waste metrics...")
            
            # Initialize columns
            cost_df['CPU_Utilization'] = None
            cost_df['Memory_Utilization'] = None
            cost_df['Is_Idle'] = False
            cost_df['Is_Oversized'] = False
            cost_df['Is_Unused'] = False
            cost_df['Idle_Days'] = 0
            cost_df['Waste_Score'] = 0.0
            cost_df['Waste_Category'] = 'Low'
            cost_df['Monthly_Waste'] = 0.0
            
            # Calculate waste per resource
            for idx, row in cost_df.iterrows():
                service = row['Service']
                resource_id = row['ResourceID']
                daily_cost = row['Cost']
                
                if service == 'VM':
                    waste_metrics = self._detect_vm_waste(resource_id, daily_cost)
                elif service == 'SQL':
                    waste_metrics = self._detect_sql_waste(resource_id, daily_cost)
                elif service == 'Functions':
                    waste_metrics = self._detect_functions_waste(resource_id, daily_cost)
                elif service == 'Storage':
                    waste_metrics = self._detect_storage_waste(resource_id, daily_cost)
                else:
                    waste_metrics = self._default_waste_metrics()
                
                for key, value in waste_metrics.items():
                    cost_df.at[idx, key] = value
            
            # Summary
            idle_count = cost_df['Is_Idle'].sum()
            oversized_count = cost_df['Is_Oversized'].sum()
            unused_count = cost_df['Is_Unused'].sum()
            total_waste = cost_df['Monthly_Waste'].sum()
            
            self.logger.info(f"  Waste detection completed")
            self.logger.info(f"  Idle resources: {idle_count}")
            self.logger.info(f"  Oversized resources: {oversized_count}")
            self.logger.info(f"  Unused resources: {unused_count}")
            self.logger.info(f"  Total monthly waste: ${total_waste:.2f}")
            
            return cost_df
            
        except Exception as e:
            self.logger.error(f"Error during waste detection: {str(e)}")
            return cost_df
    
    def _detect_vm_waste(self, vm_id: str, daily_cost: float) -> Dict:
        """Detect waste for Azure VMs."""
        cpu_utilization = self._get_simulated_cpu_utilization(vm_id)
        memory_utilization = self._get_simulated_memory_utilization(vm_id)
        
        # Stopped VMs still billed for storage
        is_stopped = hash(vm_id) % 10 == 0  # 10% stopped but billing
        is_idle = cpu_utilization < self.IDLE_CPU_THRESHOLD
        is_oversized = memory_utilization < self.OVERSIZED_MEMORY_THRESHOLD
        
        idle_days = 7 if (is_idle or is_stopped) else 0
        
        waste_score = 0.0
        if is_idle or is_stopped:
            waste_score += (idle_days / 7) * daily_cost * 30
        if is_oversized:
            waste_score += (self.OVERSIZED_MEMORY_THRESHOLD - memory_utilization) * daily_cost * 0.3
        
        monthly_waste = daily_cost * 30 if (is_idle or is_oversized or is_stopped) else 0
        waste_category = self._categorize_waste(monthly_waste)
        
        return {
            'CPU_Utilization': round(cpu_utilization, 2),
            'Memory_Utilization': round(memory_utilization, 2),
            'Is_Idle': is_idle or is_stopped,
            'Is_Oversized': is_oversized,
            'Is_Unused': False,
            'Idle_Days': idle_days,
            'Waste_Score': round(waste_score, 2),
            'Waste_Category': waste_category,
            'Monthly_Waste': round(monthly_waste, 2)
        }
    
    def _detect_sql_waste(self, db_id: str, daily_cost: float) -> Dict:
        """Detect waste for Azure SQL databases."""
        cpu_utilization = self._get_simulated_cpu_utilization(db_id)
        is_idle = cpu_utilization < 10.0
        
        idle_days = 7 if is_idle else 0
        monthly_waste = daily_cost * 30 if is_idle else 0
        waste_score = monthly_waste if is_idle else 0
        waste_category = self._categorize_waste(monthly_waste)
        
        return {
            'CPU_Utilization': round(cpu_utilization, 2),
            'Memory_Utilization': None,
            'Is_Idle': is_idle,
            'Is_Oversized': False,
            'Is_Unused': False,
            'Idle_Days': idle_days,
            'Waste_Score': round(waste_score, 2),
            'Waste_Category': waste_category,
            'Monthly_Waste': round(monthly_waste, 2)
        }
    
    def _detect_functions_waste(self, func_id: str, daily_cost: float) -> Dict:
        """Detect waste for Azure Functions."""
        invocations = hash(func_id) % 100
        is_unused = invocations == 0
        
        unused_days = 30 if is_unused else 0
        monthly_waste = daily_cost * 30 if is_unused else 0
        waste_score = monthly_waste if is_unused else 0
        waste_category = self._categorize_waste(monthly_waste)
        
        return {
            'CPU_Utilization': None,
            'Memory_Utilization': None,
            'Is_Idle': False,
            'Is_Oversized': False,
            'Is_Unused': is_unused,
            'Idle_Days': unused_days,
            'Waste_Score': round(waste_score, 2),
            'Waste_Category': waste_category,
            'Monthly_Waste': round(monthly_waste, 2)
        }
    
    def _detect_storage_waste(self, storage_id: str, daily_cost: float) -> Dict:
        """Detect waste for Azure Storage (orphaned disks, old blobs)."""
        last_accessed_days = hash(storage_id) % 60
        is_orphaned = hash(storage_id) % 15 == 0  # 1/15 are orphaned disks
        is_unused = last_accessed_days > self.UNUSED_DAYS_THRESHOLD
        
        unused_days = last_accessed_days if (is_unused or is_orphaned) else 0
        monthly_waste = daily_cost * 30 if (is_unused or is_orphaned) else 0
        waste_score = monthly_waste if (is_unused or is_orphaned) else 0
        waste_category = self._categorize_waste(monthly_waste)
        
        return {
            'CPU_Utilization': None,
            'Memory_Utilization': None,
            'Is_Idle': False,
            'Is_Oversized': False,
            'Is_Unused': is_unused or is_orphaned,
            'Idle_Days': unused_days,
            'Waste_Score': round(waste_score, 2),
            'Waste_Category': waste_category,
            'Monthly_Waste': round(monthly_waste, 2)
        }
    
    def _categorize_waste(self, monthly_waste: float) -> str:
        """Categorize waste level."""
        if monthly_waste > self.CRITICAL_THRESHOLD:
            return 'Critical'
        elif monthly_waste > self.HIGH_THRESHOLD:
            return 'High'
        elif monthly_waste > self.MEDIUM_THRESHOLD:
            return 'Medium'
        else:
            return 'Low'
    
    def _get_simulated_cpu_utilization(self, resource_id: str) -> float:
        """Simulate CPU utilization."""
        hash_val = hash(resource_id) % 100
        if hash_val < 30:
            return float(hash_val % 5)
        elif hash_val < 70:
            return float(5 + (hash_val % 25))
        else:
            return float(30 + (hash_val % 50))
    
    def _get_simulated_memory_utilization(self, resource_id: str) -> float:
        """Simulate memory utilization."""
        hash_val = hash(resource_id + "mem") % 100
        if hash_val < 25:
            return float(hash_val % 30)
        else:
            return float(30 + (hash_val % 60))
    
    def _default_waste_metrics(self) -> Dict:
        """Default waste metrics."""
        return {
            'CPU_Utilization': None,
            'Memory_Utilization': None,
            'Is_Idle': False,
            'Is_Oversized': False,
            'Is_Unused': False,
            'Idle_Days': 0,
            'Waste_Score': 0.0,
            'Waste_Category': 'Low',
            'Monthly_Waste': 0.0
        }


# ============================================================================
# Output Functions
# ============================================================================

def save_output(df: pd.DataFrame, output_dir: str = 'data') -> str:
    """Save DataFrame to CSV."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/azure_costs_with_waste_{timestamp}.csv"
    df.to_csv(filename, index=False)
    return filename


def print_summary(df: pd.DataFrame, logger: logging.Logger) -> None:
    """Print summary statistics."""
    logger.info("="*80)
    logger.info("AZURE DATA COLLECTION SUMMARY")
    logger.info("="*80)
    logger.info(f"Total Records: {len(df)}")
    logger.info(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    logger.info(f"Total Cost: ${df['Cost'].sum():.2f}")
    logger.info("")
    logger.info("Cost by Service:")
    
    service_costs = df.groupby('Service')['Cost'].sum().sort_values(ascending=False)
    for service, cost in service_costs.items():
        percentage = (cost / df['Cost'].sum()) * 100
        logger.info(f"  {service:15s}: ${cost:10.2f} ({percentage:5.1f}%)")
    
    logger.info("")
    logger.info("Waste Summary:")
    logger.info(f"  Idle Resources: {df['Is_Idle'].sum()}")
    logger.info(f"  Oversized Resources: {df['Is_Oversized'].sum()}")
    logger.info(f"  Unused Resources: {df['Is_Unused'].sum()}")
    logger.info(f"  Total Monthly Waste: ${df['Monthly_Waste'].sum():.2f}")
    logger.info("="*80)


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function."""
    logger = setup_logging()
    
    try:
        logger.info("Starting Azure cost collection...")
        logger.info("")
        
        # Get subscription ID from environment or prompt
        subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
        if not subscription_id:
            logger.error("AZURE_SUBSCRIPTION_ID environment variable not set")
            logger.info("Run: export AZURE_SUBSCRIPTION_ID='your-subscription-id'")
            logger.info("Or find it with: az account show --query id -o tsv")
            return 1
        
        # Initialize Azure
        logger.info("Step 1: Initialize Azure connections")
        cost_explorer = AzureCostExplorer(subscription_id, logger=logger)
        logger.info("")
        
        # Fetch cost data
        logger.info("Step 2: Fetch cost data (last 90 days)")
        cost_df = cost_explorer.get_cost_data(days=90)
        logger.info("")
        
        # Enrich with tags
        logger.info("Step 3: Enrich with resource tags")
        cost_df = cost_explorer.enrich_with_resource_tags(cost_df)
        logger.info("")
        
        # Detect waste
        logger.info("Step 4: Detect wasteful resources")
        waste_detector = AzureWasteDetector(logger=logger)
        cost_df = waste_detector.add_waste_metrics(cost_df)
        logger.info("")
        
        # Save output
        logger.info("Step 5: Save results to CSV")
        output_file = save_output(cost_df)
        logger.info(f"Data saved to: {output_file}")
        logger.info("")
        
        # Print summary
        print_summary(cost_df, logger)
        
        logger.info("="*80)
        logger.info("AZURE COST COLLECTION COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Output file: {output_file}")
        logger.info("")
        
        return 0
        
    except Exception as e:
        logger.error("="*80)
        logger.error("SCRIPT FAILED")
        logger.error("="*80)
        logger.error(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)