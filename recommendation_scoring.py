import pandas as pd
from sqlalchemy import create_engine
from db import engine, write_data_to_db
import numpy as np
import sqlalchemy


# Load the data from MySQL tables
def load_data():
    buyer_df = pd.read_sql("SELECT * FROM buyers", engine)
    suppliers_df = pd.read_sql("SELECT * FROM suppliers", engine)
    return buyer_df, suppliers_df


# Create matching function for the combined supplier data
def match_materials_to_buyers(buyer_df, combined_supplier_df, number_of_matches):
    # Create a results dataframe to store matches
    results = []
    
    # For each buyer
    for _, buyer in buyer_df.iterrows():
        buyer_id = buyer['buyer_id']
        preferred_grade = buyer['preferred_grade']
        preferred_finish = buyer['preferred_finish']
        preferred_thickness = buyer['preferred_thickness_mm']
        preferred_width = buyer['preferred_width_mm']
        max_weight = buyer['max_weight_kg']
        min_quantity = buyer['min_quantity']
    
        # Find weight and quantity matches from all suppliers
        base_matches = combined_supplier_df[
            (combined_supplier_df['weight_kg'] <= max_weight) &
            (combined_supplier_df['quantity'] >= min_quantity) 
        ].copy()
    
        if len(base_matches) > 0:
            # Calculate match scores
            # Initialize scores
            base_matches['grade_score'] = 0.0
            base_matches['finish_score'] = 0.0
            base_matches['thickness_score'] = 0.0
            base_matches['width_score'] = 0.0
            base_matches['weight_score'] = 0.0
            
            # Grade score and finish score (1 if exact match, 0 otherwise)
            
            base_matches['grade_score'] = 0
            base_matches.loc[base_matches['grade']==preferred_grade, 'grade_score'] = 1 
            
            base_matches['finish_score'] = 0
            base_matches.loc[base_matches['finish_score']==preferred_finish, 'finish_score'] = 1 
            
            # Thickness score (for supplier1 only)
            # Only calculate for rows with non-null thickness
            thickness_rows = base_matches['thickness_mm'].notna()
            if thickness_rows.any():
                thickness_subset = base_matches.loc[thickness_rows, 'thickness_mm']
                thickness_diff = abs(thickness_subset - preferred_thickness)
                # normalize the score
                thickness_diff_normalize = thickness_diff / thickness_diff.max()
                # penalize the high difference
                base_matches.loc[thickness_rows, 'thickness_score'] = 1 - thickness_diff_normalize
            
            # Calculate overall match score
            base_matches['match_score'] = (
                base_matches['grade_score'] * 30 +
                base_matches['finish_score'] * 20 +
                base_matches['thickness_score'] * 50
            )

            # Sort by match score, prioritizing supplier1 for ties (since it has more complete data)
            base_matches = base_matches.sort_values(['match_score', 'supplier_id'], ascending=[False, True])
            
            # Select top n matches overall
            top_matches = base_matches.head(number_of_matches)
            
            # Add to results to form a dict of all matches
            for _, match in top_matches.iterrows():
                
                results.append({
                    'buyer_id': buyer_id,
                    'supplier_id': match['supplier_id'],
                    'match_score': round(match['match_score'], 2),
                    'article_id': match['article_id'],
                    'grade': match['grade'],
                    'material': match['material'],
                    'finish': match['finish'],
                    'thickness_mm': match['thickness_mm'],
                    'width_mm': match['width_mm'],
                    'weight_kg': match['weight_kg'],
                    'quantity': match['quantity'],
                    'quality_choice': match['quality_choice'],
                    'reserved_status': match['reserved_status']
                })
    
    # Convert results to dataframe
    results_df = pd.DataFrame(results)
    return results_df



# Main pipeline function
def run_material_matching_pipeline(number_of_matches):
    print("Loading data...")
    buyer_df, supplier_df = load_data()
    
    print("Matching materials to buyers...")
    results_df = match_materials_to_buyers(buyer_df, supplier_df, number_of_matches)

    print("Generating recommendation table...")
    results_df = results_df.sort_values(['buyer_id', 'match_score'], ascending=[True, False])
    
    return results_df



# Run the pipeline and display the results
if __name__ == "__main__":
    number_of_matches = 3
    combined_recommendations = run_material_matching_pipeline(number_of_matches)
    print(combined_recommendations)
    write_data_to_db(combined_recommendations, 'recommendations_with_score', check_if_exists=False)

