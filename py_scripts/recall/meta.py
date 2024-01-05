def meta(meta_data):
    
    default_metadata = {
                
                'licence_full_name' : 'Unknown',
                'licence_short_name' : 'Unknown',
                'licence_legal_code' : 'Unknown',
                'classification_count' : 'Unknown',
                'classification_id' : 'Unknown',
                'orpha_number' : 'Unknown',
                'classification_name' : 'Unknown'
                
            }
    meta_data.update(default_metadata)
    
    return meta_data