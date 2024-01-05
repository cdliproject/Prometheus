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
from dataclasses import dataclass, field
import h5py
import numpy as np
from recall.meta import meta


orphanet_raw_xml = '../orphanet_data/orphanet_xml'
directory_to_hdf5 = '../orphanet_data/orphanet_hdf5'
directory_to_csv = '../orphanet_data/orphanet_csv'

@dataclass
class Disease:
    
    classification_id: int
    disorder_id: int
    disorder_type: str
    orpha_code: int
    name: str
    expert_link: str
    meta_data: dict = field(default_factory=dict)
    
    ## additional fileds as needed can be added here    
    # genetic_inforrmation: str = None
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
        self.xml_file = xml_file
        self.tree = ET.parse(self.xml_file)
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
        
        initial_meta_data = {}
        metadata = meta(initial_meta_data)
        
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
    
    
    def iter_parse_diseases(self):
        
        for event, elem in ET.iterparse(self.xml_file, events=('end',), tag='Disorder'):
            disorder_id_str = elem.attrib.get('id', 'Unknown')
            classification_id = 0
            
            if disorder_id_str != 'Unknown':
                try:
                    disorder_id = int(disorder_id_str)
                except ValueError:
                    print(f'Error: {disorder_id_str} is not a valid disorder id')
                    disorder_id = 0
                    
                    
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
            
            disease =\
            \
            Disease(
                classification_id=int(classification_id),\
                disorder_type=disorder_id,\
                disorder_id=disorder_type,\
                orpha_code=int(orpha_code) if orpha_code != 'Unknown' else 0,\
                name=name,\
                expert_link=expert_link,\
                meta_data=meta_data
                )

            yield disease
            elem.clear()
        
        
def get_xml_files(directory_to):
    
    return [os.path.join(root, file)\
        for root, _, files in os.walk(directory_to)\
            for file in files if file.endswith('.xml')]


def write_to_hdf5(diseases, hdf5_file_path):
    
    with h5py.File(hdf5_file_path, 'w') as hdf_file:
        for disease in diseases:
            group = hdf_file.create_group(str(disease.disorder_id))
            for key, value in disease.to_dict().items():
                if isinstance(value, (int, str)):
                    value = [value]
                
                value_array = np.array(value)
                group.create_dataset(key, data=value_array)
                
                
def main():
    
    os.makedirs(directory_to_hdf5, exist_ok=True)
    xml_files = get_xml_files(orphanet_raw_xml)
    
    for xml_file in tqdm(xml_files, desc="Processing XML files"):
        parser = OrphanetXMLParser(xml_file)
        hdf5_file_path = os.path.splitext(xml_file)[0] + '.h5'
        
        write_to_hdf5(parser.iter_parse_diseases(), hdf5_file_path)


if __name__ == '__main__':
    main()
    

