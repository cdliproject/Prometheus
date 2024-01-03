"""
useful urls = 
            
            'https://stackoverflow.com/questions/2869564/xml-filtering-with-python'
            'https://stackoverflow.com/questions/1912434/how-do-i-parse-xml-in-python'

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
from tqdm import tqdm


raw_data_path = './in_xml'

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
        
        metadata = {
            
            'licence_full_name' : 'Unknown',
            'licence_short_name' : 'Unknown',
            'licence_legal_code' : 'Unknown',
            'classification_count' : 'Unknown',
            'classification_id' : 'Unknown',
            'orpha_number' : 'Unknown',
            'classification_name' : 'Unknown'
            
        }
        
        licence = self.root.find('.//Licence')
        if licence is not None:
            metadata['licence_full_name'] = licence.findtext('FullName', default='Unknown')
            metadata['licence_short_name'] = licence.findtext('ShortIdentifier', default='Unknown')
            metadata['licence_legal_code'] = licence.findtext('LegalCode', default='Unknown')
            
        classification_list = self.root.find('.//ClassificationList')
        if classification_list is not None:
            metadata['classification_count'] = classification_list.attrib.get('count', 'Unknown')

            classification = classification_list.find('.//Classification')
            if classification is not None:
                metadata['classification_id'] = classification.attrib.get('id','unknown')
                metadata['orpha_number'] = classification.findtext('OrphaNumber', default='Unknown')
                metadata['classification_name'] = classification.findtext('Name', default='Unknown')
        
        return metadata
        
    def parse_diseases(self):
        
        diseases = [] # list of diseases
        
        classification_id = self.root.find('.//ClassificationList/Classification')
        classification_id = classification_id.attrib.get('id','Unknown')\
            if classification_id is not None else 'Unknown'
        
            
        for clls in self.root.findall('.//ClassificationNode'): # find all the disorders, nested in the root 

                        
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

xml_file = None

for root, dirs, files in os.walk(raw_data_path):
    for file in files:
        if file.endswith('.xml'):
            xml_file = os.path.join(root, file)
            break
        if xml_file:
            break
        
if xml_file is None:
    print('No XML file found')
    exit()
    

parser = OrphanetXMLParser(xml_file)


diseases = parser.parse_diseases()

diseases_df = pd.DataFrame([disease.to_dict() for disease in diseases])
print(diseases_df)


def main():

    directory = 'test_out/diseases_data'
    os.makedirs(directory, exist_ok=True)

    xml_files = [os.path.join(root, file) for root, dirs, files in os.walk(raw_data_path)\
        for file in files if file.endswith('.xml')]

    all_diseases_df = pd.DataFrame()
    
    for xml_file in tqdm(xml_files, desc="Processing XML Files..."):
        parser = OrphanetXMLParser(xml_file)
        diseases = parser.parse_diseases()
        diseases_df = pd.DataFrame([disease.to_dict() for disease in diseases])
        all_diseases_df = pd.concat([all_diseases_df, diseases_df], ignore_index=True)

        
    combined_file_path = os.path.join(directory, 'combined_diseases.csv')
    all_diseases_df.to_csv(combined_file_path, index=False)
    print(f'Combined data saved to {combined_file_path}')
    

if __name__ == '__main__':
    main()
    

