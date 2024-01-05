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

import lxml.etree as ET
import os
from tqdm import tqdm
from dataclasses import dataclass
import h5py
import numpy as np



orphanet_raw_xml = '../orphanet_data/orphanet_xml'
directory_to_hdf5 = '../orphanet_data/orphanet_hdf5'
directory_to_csv = '../orphanet_data/orphanet_csv'

class Disease:
    
    classification_id: int
    disorder_id: int
    disorder_type: str
    orpha_code: int
    name: str
    expert_link: str
    meta_data: dict
    
    ## additional fileds as needed can be added here
    
    # genetic_inforrmation: str -> None
    # symptomology: str -> None
    #...
    
        
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
    
    def iter_parse_diseases(self):
        
        for _, elem in ET.etree.iterparse(self.xml_file, events=('end',), tag='Disorder'):
            disorder_id = elem.attrib.get('id', 'unknown') # find the disorder id, nested in the disorder
            disorder_type = elem.find('.//DisorderType/Name').text\
                if elem.find('.//DisorderType/Name') is not None else 'Unknown' # find the disorder type, nested in the disorder
            orpha_code = elem.find('.//OrphaCode').text\
                if elem.find('.//OrphaCode') is not None else 'Unknown' # find the orpha code, nested in the disorder
            name = elem.find('.//Name').text\
                if elem.find('.//Name') is not None else 'Unknown' # find the name, nested in the disorder
            expert_link = elem.find('.//ExpertLink').text\
                if elem.find('.//ExpertLink') is not None else 'Unknown' # find the expert link, nested in the disorder
            classification_id = 'Extracted ID'
            meta_data = self.extract_meta_data()
            
            diseases =\
            Disease(
                classification_id, disorder_type,\
                disorder_id, orpha_code,\
                name, expert_link,\
                meta_data
                )

            yield diseases
            elem.clear()
        
        
def get_xml_files(directory_to):
    
    return [os.path.join(root, file)\
        for root, _, files in os.walk(directory_to)\
            for file in files if file.endswith('.xml')]


def write_to_hdf5(diseases, hdf5_file_path):
    
    with h5py.File(hdf5_file_path, 'w') as hdf_file:
        for disease in diseases:
            group = hdf_file.create_group(str(disease.disorder_id))
            disease_dict = disease.to_dict()
            for key, value in disease.to_dict().items():
                group.create_dataset(key, data=np.array(value, dtype='S'))
                
                
    

def main():
    os.makedirs(directory_to_hdf5, exist_ok=True)
    xml_files = get_xml_files(orphanet_raw_xml)
    
    for xml_file in tqdm(xml_files, desc="Processing XML files"):
        parser = OrphanetXMLParser(xml_file)
        hdf5_file_path = os.path.splitext(xml_file)[0] + '.h5'
        
        write_to_hdf5(parser.iter_parse_diseases(), hdf5_file_path)


if __name__ == '__main__':
    main()
    

