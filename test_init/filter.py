"""
url = 'https://stackoverflow.com/questions/2869564/xml-filtering-with-python'

@themkdemiiir stated:

    The project involves creating a new parser to effectively 
    manage and integrate the comprehensive rare diseases dataset
    from Orphanet, which is available on their Orphanet Scientific Knowledge
    Files website. This data is rich in content, including rare disease alignments
    with various medical classifications like SNOMED CT, ICD-10, ICD-11, OMIM, UMLS, MeSH, MedDRA, and GARD. 
    In addition, it provides detailed information on classifications of 
    rare diseases, their linearisation, associated genes, clinical signs and symptoms,
    functional consequences, epidemiology, and natural history.
    We aim to develop a parser to handle these diverse data types and
    bring them into a unified format that fits our system's needs.
    This will significantly enhance our capacity to utilize the Orphanet
    dataset for advanced analysis and application in rare disease studies.


We are going to use the ElementTree lib to parse the XML file.
The data 'en_product3_181.xml' is a 4.1MB for rare neurological diseases.

"""
import pandas as pd
import xml.etree.ElementTree as ET
import os
import tqdm as tqdm

class Disease:
    
    def __init__(self,
                 classification_id, disorder_type, disorder_id,
                 orpha_code, name, expert_link, meta_data):
        
        self.classification_id = classification_id
        self.disorder_id = disorder_id
        self.disorder_type = disorder_type
        self.orpha_code = orpha_code
        self.name = name
        self.expert_link = expert_link
        self.meta_data = meta_data
        
    def to_dict(self):
        return {
            'ClassificationID': self.classification_id,
            'DisorderID': self.disorder_id,
            'DisorderType' : self.disorder_type,
            'OrphaCode': self.orpha_code,
            'Name': self.name,
            'ExpertLink': self.expert_link,
            'MetaData': self.meta_data
        }
        
class OrphanetXMLParser:
    def __init__(self,xml_file): # tree structure of the xml file
        self.tree = ET.parse(xml_file)
        self.root = self.tree.getroot()
        self.meta_data = self.extract_meta_data()
        
    def extract_meta_data(self):
        """
        will be attributing meta_data from XML file to further
        analyse the data and find the most relevant interlinks
        for our project.
        These meta_datas can be Licence information, date of JDBOR,
        or Orpha Number (dont confuse it with orpha code).
        """
        
        meta_data_parse = []
        
        for meta in self.root.findall('.//Availability'):
            
            full_name = meta.find('FullName').text\
                if meta.find('FullName') is not None else 'Unknown'
                
            short_identifier = meta.find('ShortIdentifier').text\
                if meta.find('ShortIdentifier') is not None else 'Unknown'
                
        self.meta_data =\
            { full_name : meta_data_parse,
                short_identifier : meta_data_parse
                
        }
        
    def parse_diseases(self):
        diseases = [] # list of diseases
        
            
        for clls in self.root.findall('.//ClassificationNode'): # find all the disorders, nested in the root 

            classification_id = clls.attrib.get('id', 'unknown')
            
            disorder_id = clls.find('.//Disorder').attrib.get('id', 'unknown')\
                if clls.find('.//Disorder') is not None else 'Unknown' # find the disorder id, nested in the disorder
                
            disorder_type = clls.find('.//DisorderType/Name').text\
                if clls.find('.//DisorderType/Name') is not None else 'Unknown' # find the disorder type, nested in the disorder
            
            orpha_code = clls.find('.//OrphaCode').text\
                if clls.find('.//OrphaCode') is not None else 'Unknown' # find the orpha code, nested in the disorder
            
            name = clls.find('.//Name').text\
                if clls.find('.//Name') is not None else 'Unknown' # find the name, nested in the disorder
                
            expert_link = clls.find('.//ExpertLink').text\
                if clls.find('.//ExpertLink') is not None else 'Unknown' # find the expert link, nested in the disorder
            
            disease = Disease( classification_id, disorder_type, disorder_id,\
                            orpha_code, name, expert_link, self.meta_data)    # create a disease object
            
            diseases.append(disease)
        
        return diseases
    
parser = OrphanetXMLParser('./in_xml/en_product3_181.xml')

diseases = parser.parse_diseases()

diseases_df = pd.DataFrame([disease.to_dict() for disease in diseases])
print(diseases_df)

def main():
    directory = 'test_out/diseases_data'
    os.makedirs(directory, exist_ok=True)
    
    file_number = 1
    while os.path.exists(os.path.join(directory, f'diseases{file_number}.csv')):
        file_number += 1
        
    file_path = os.path.join(directory, f'diseases{file_number}.csv')
    diseases_df.to_csv(file_path, index=False)
    print(f'File saved to {file_path}')

if __name__ == '__main__':
    main()
    

