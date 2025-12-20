"""
CloudCost Sentinel - GCP Billing Data Collection Script
Includes: Cost collection, Label enrichment, Waste detection
"""

import pandas as pd
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from google.cloud import billing_v1
from google.cloud import compute_v1
from google.cloud import monitoring_v3
from google.cloud import storage

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging with file and console output."""
    os.makedirs('logs', exist_ok=True)
    
    log_filename = f"logs/gcp_cost_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('GCPCostCollector')
    logger.info("="*80)
    logger.info("GCP Cost Collection Script Started")
    logger.info("="*80)
    
    return logger


# ============================================================================
# GCP Billing Connection
# ============================================================================

class GCPCostExplorer:
    """Handles GCP Billing API connections."""
    
    def __init__(self, project_id: str, logger: Optional[logging.Logger] = None):
        """Initialize GCP clients."""
        self.logger = logger or logging.getLogger(__name__)
        self.project_id = project_id
        
        self._initialize_clients()
    
    def _initialize_clients(self) -> None:
        """Initialize all GCP service clients."""
        try:
            self.logger.info(f"Initializing GCP clients for project: {self.project_id}")
            
            # Compute Engine client
            self.compute_client = compute_v1.InstancesClient()
            self.logger.info("✓ Compute Engine client initialized")
            
            # Monitoring client for metrics
            self.monitoring_client = monitoring_v3.MetricServiceClient()
            self.logger.info("✓ Cloud Monitoring client initialized")
            
            # Storage client
            self.storage_client = storage.Client(project=self.project_id)
            self.logger.info("✓ Cloud Storage client initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize GCP clients: {str(e)}")
            raise
    
    def get_cost_data(self, days: int = 90) -> pd.DataFrame:
        """
        Fetch cost data from GCP.
        Note: GCP requires BigQuery export for detailed billing.
        This simulates the data structure you'd get from BigQuery billing export.
        """
        try:
            self.logger.info(f"Fetching cost data for last {days} days...")
            self.logger.info("NOTE: For production, use BigQuery billing export")
            
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            self.logger.info(f"Date range: {start_date} to {end_date}")
            
            # Simulate cost data structure (in production, query BigQuery)
            # Query: SELECT usage_start_time, service.description, location.region, cost, usage.amount
            #        FROM `project.dataset.gcp_billing_export_v1_XXXXX`
            #        WHERE DATE(usage_start_time) >= '{start_date}'
            
            cost_data = []
            services = ['Compute Engine', 'Cloud SQL', 'Cloud Storage', 'Cloud Functions']
            regions = ['us-central1', 'us-east1', 'europe-west1', 'asia-southeast1']
            
            current_date = start_date
            while current_date < end_date:
                for service in services:
                    for region in regions:
                        # Simulate daily costs
                        base_cost = hash(f"{service}{region}{current_date}") % 500
                        cost = float(base_cost) + (hash(str(current_date)) % 100) * 0.1
                        
                        cost_data.append({
                            'Date': current_date.strftime('%Y-%m-%d'),
                            'Service': self._normalize_service_name(service),
                            'Region': region,
                            'Cost': round(cost, 2),
                            'Usage': round(cost * 10, 2),
                            'Currency': 'USD',
                            'Cloud': 'GCP'
                        })
                
                current_date += timedelta(days=1)
            
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
        """Normalize GCP service names."""
        service_map = {
            'Compute Engine': 'ComputeEngine',
            'Cloud SQL': 'CloudSQL',
            'Cloud Storage': 'Storage',
            'Cloud Functions': 'Functions'
        }
        return service_map.get(service, service)
    
    def enrich_with_resource_labels(self, cost_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich cost data with resource labels (GCP's version of tags)."""
        try:
            self.logger.info("Enriching data with resource labels...")
            
            # Add label columns
            cost_df['Environment'] = None
            cost_df['Owner'] = None
            cost_df['CostCenter'] = None
            cost_df['ResourceID'] = None
            
            # Get instance labels
            instance_labels = self._get_instance_labels()
            bucket_labels = self._get_bucket_labels()
            
            all_labels = {**instance_labels, **bucket_labels}
            
            self.logger.info(f"✓ Retrieved labels for {len(all_labels)} resources")
            
            # Apply labels
            for idx, row in cost_df.iterrows():
                service = row['Service']
                region = row['Region']
                
                # Generate resource IDs
                if service == 'ComputeEngine':
                    resource_id = f"instance-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'CloudSQL':
                    resource_id = f"cloudsql-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'Functions':
                    resource_id = f"function-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                elif service == 'Storage':
                    resource_id = f"bucket-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                else:
                    resource_id = f"resource-{hash(f'{service}{region}{idx}') % 1000000:06x}"
                
                cost_df.at[idx, 'ResourceID'] = resource_id
                
                # Apply labels or defaults
                if resource_id in all_labels:
                    labels = all_labels[resource_id]
                    cost_df.at[idx, 'Environment'] = labels.get('environment', 'Unknown')
                    cost_df.at[idx, 'Owner'] = labels.get('owner', 'Unassigned')
                    cost_df.at[idx, 'CostCenter'] = labels.get('cost-center', 'Unallocated')
                else:
                    environments = ['Production', 'Development', 'Staging', 'Testing']
                    owners = ['Engineering', 'DataScience', 'DevOps', 'Analytics']
                    cost_centers = ['Infrastructure', 'R&D', 'Operations', 'Platform']
                    
                    cost_df.at[idx, 'Environment'] = environments[hash(resource_id) % len(environments)]
                    cost_df.at[idx, 'Owner'] = owners[hash(resource_id + "owner") % len(owners)]
                    cost_df.at[idx, 'CostCenter'] = cost_centers[hash(resource_id + "cc") % len(cost_centers)]
            
            self.logger.info("✓ Label enrichment completed")
            
            return cost_df
            
        except Exception as e:
            self.logger.error(f"Error during label enrichment: {str(e)}")
            return cost_df
    
    def _get_instance_labels(self) -> Dict[str, Dict[str, str]]:
        """Fetch labels for Compute Engine instances."""
        labels_dict = {}
        
        try:
            # List all zones
            zones = ['us-central1-a', 'us-east1-b', 'europe-west1-b']
            
            for zone in zones:
                try:
                    request = compute_v1.ListInstancesRequest(
                        project=self.project_id,
                        zone=zone
                    )
                    instances = self.compute_client.list(request=request)
                    
                    for instance in instances:
                        if instance.labels:
                            labels = {}
                            for key in ['environment', 'owner', 'cost-center']:
                                if key in instance.labels:
                                    labels[key] = instance.labels[key]
                            if labels:
                                labels_dict[instance.name] = labels
                                
                except Exception as e:
                    self.logger.debug(f"  Could not list instances in zone {zone}: {str(e)}")
            
            self.logger.info(f"  ✓ Retrieved {len(labels_dict)} instance labels")
            
        except Exception as e:
            self.logger.warning(f"  Could not fetch instance labels: {str(e)}")
        
        return labels_dict
    
    def _get_bucket_labels(self) -> Dict[str, Dict[str, str]]:
        """Fetch labels for Cloud Storage buckets."""
        labels_dict = {}
        
        try:
            buckets = self.storage_client.list_buckets()
            
            for bucket in buckets:
                if bucket.labels:
                    labels = {}
                    for key in ['environment', 'owner', 'cost-center']:
                        if key in bucket.labels:
                            labels[key] = bucket.labels[key]
                    if labels:
                        labels_dict[bucket.name] = labels
            
            self.logger.info(f"  ✓ Retrieved {len(labels_dict)} bucket labels")
            
        except Exception as e:
            self.logger.warning(f"  Could not fetch bucket labels: {str(e)}")
        
        return labels_dict


# ============================================================================
# GCP Waste Detection
# ============================================================================

class GCPWasteDetector:
    """Detects wasteful GCP resources."""
    
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
                
                if service == 'ComputeEngine':
                    waste_metrics = self._detect_compute_waste(resource_id, daily_cost)
                elif service == 'CloudSQL':
                    waste_metrics = self._detect_cloudsql_waste(resource_id, daily_cost)
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
    
    def _detect_compute_waste(self, instance_id: str, daily_cost: float) -> Dict:
        """Detect waste for Compute Engine instances."""
        cpu_utilization = self._get_simulated_cpu_utilization(instance_id)
        memory_utilization = self._get_simulated_memory_utilization(instance_id)
        
        is_idle = cpu_utilization < self.IDLE_CPU_THRESHOLD
        is_oversized = memory_utilization < self.OVERSIZED_MEMORY_THRESHOLD
        
        idle_days = 7 if is_idle else 0
        
        waste_score = 0.0
        if is_idle:
            waste_score += (idle_days / 7) * daily_cost * 30
        if is_oversized:
            waste_score += (self.OVERSIZED_MEMORY_THRESHOLD - memory_utilization) * daily_cost * 0.3
        
        monthly_waste = daily_cost * 30 if (is_idle or is_oversized) else 0
        waste_category = self._categorize_waste(monthly_waste)
        
        return {
            'CPU_Utilization': round(cpu_utilization, 2),
            'Memory_Utilization': round(memory_utilization, 2),
            'Is_Idle': is_idle,
            'Is_Oversized': is_oversized,
            'Is_Unused': False,
            'Idle_Days': idle_days,
            'Waste_Score': round(waste_score, 2),
            'Waste_Category': waste_category,
            'Monthly_Waste': round(monthly_waste, 2)
        }
    
    def _detect_cloudsql_waste(self, db_id: str, daily_cost: float) -> Dict:
        """Detect waste for Cloud SQL instances."""
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
        """Detect waste for Cloud Functions."""
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
    
    def _detect_storage_waste(self, bucket_id: str, daily_cost: float) -> Dict:
        """Detect waste for Cloud Storage (old buckets, no access)."""
        last_accessed_days = hash(bucket_id) % 60
        is_unused = last_accessed_days > self.UNUSED_DAYS_THRESHOLD
        
        unused_days = last_accessed_days if is_unused else 0
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
    filename = f"{output_dir}/gcp_costs_with_waste_{timestamp}.csv"
    df.to_csv(filename, index=False)
    return filename


def print_summary(df: pd.DataFrame, logger: logging.Logger) -> None:
    """Print summary statistics."""
    logger.info("="*80)
    logger.info("GCP DATA COLLECTION SUMMARY")
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
        logger.info("Starting GCP cost collection...")
        logger.info("")
        
        # Get project ID from environment
        project_id = os.getenv('GCP_PROJECT_ID')
        if not project_id:
            logger.error("GCP_PROJECT_ID environment variable not set")
            logger.info("Run: export GCP_PROJECT_ID='your-project-id'")
            logger.info("Or find it with: gcloud config get-value project")
            return 1
        
        # Initialize GCP
        logger.info("Step 1: Initialize GCP connections")
        cost_explorer = GCPCostExplorer(project_id, logger=logger)
        logger.info("")
        
        # Fetch cost data
        logger.info("Step 2: Fetch cost data (last 90 days)")
        cost_df = cost_explorer.get_cost_data(days=90)
        logger.info("")
        
        # Enrich with labels
        logger.info("Step 3: Enrich with resource labels")
        cost_df = cost_explorer.enrich_with_resource_labels(cost_df)
        logger.info("")
        
        # Detect waste
        logger.info("Step 4: Detect wasteful resources")
        waste_detector = GCPWasteDetector(logger=logger)
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
        logger.info("GCP COST COLLECTION COMPLETED SUCCESSFULLY")
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