""" Module for generating reports from the database. """
import csv
from decimal import Decimal, InvalidOperation

from src.db import get_channel_metrics

def save_channel_metrics(db_path, output_file_path):
    """
    Calculates CPO and ROAS metrics, and saves to a new CSV.
    """
    channel_metrics = get_channel_metrics(db_path)

    # Prepare CSV writing
    with open(output_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Write header
        csvwriter.writerow([
            'channel_name', 'date', 'cost', 'ihc', 
            'ihc_revenue', 'CPO', 'ROAS'
        ])
        
        # Process each row and write to CSV
        total_cpo = Decimal('0')
        total_roas = Decimal('0')
        valid_metrics_count = 0
        
        for row in channel_metrics:
            channel_name, date, cost, ihc, ihc_revenue = row
            
            try:
                # Convert to Decimal for precise calculations
                cost = Decimal(str(cost))
                ihc = Decimal(str(ihc))
                ihc_revenue = Decimal(str(ihc_revenue))
                
                # Calculate CPO (cost per order)
                if ihc != 0:
                    cpo = round(cost / ihc, 2)
                else:
                    cpo = "N/A"
                
                # Calculate ROAS (return on ad spend)
                if cost != 0:
                    roas = round(ihc_revenue / cost, 2)
                else:
                    roas = "N/A"
                
                # Track metrics for averages
                if isinstance(cpo, Decimal) and isinstance(roas, Decimal):
                    total_cpo += cpo
                    total_roas += roas
                    valid_metrics_count += 1
                
            except (InvalidOperation, TypeError, ZeroDivisionError):
                cpo = "N/A"
                roas = "N/A"
            
            # Write row to CSV
            csvwriter.writerow([
                channel_name, date, 
                round(cost, 2) if isinstance(cost, Decimal) else cost,
                round(ihc, 2) if isinstance(ihc, Decimal) else ihc,
                round(ihc_revenue, 2) if isinstance(ihc_revenue, Decimal) else ihc_revenue,
                cpo,
                roas
            ])
    
    # Calculate and print summary statistics
    if valid_metrics_count > 0:
        avg_cpo = round(total_cpo / valid_metrics_count, 2)
        avg_roas = round(total_roas / valid_metrics_count, 2)
        print(f"\nSummary Statistics:")
        print(f"Average CPO: €{avg_cpo}")
        print(f"Average ROAS: {avg_roas}")
    
    print(f"\nSuccessfully processed data and saved to {output_file_path}")
    