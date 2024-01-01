

"""
url = 'https://stackoverflow.com/questions/2869564/xml-filtering-with-python'

@themkdemiiir said:

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

import xml.etree.ElementTree as ET
import pandas as pd

class TestParser:
    
    def __init__(self, file):
        self.file = file
        self.tree = ET.parse(self.file)
        self.root = self.tree.getroot()
        
    def parse_element(self, element):
        element_data = {"tag": element.tag, "text": element.text.strip() if element.text else None, "attributes": element.attrib}
        children_data = []
        
        for child in element:
            child_data = self.parse_element(child)
            children_data.append(child_data)
            
        if children_data:
            element_data["children"] = children_data
            
        return element_data
    
    def parse_root(self):
        return self.parse_element(self.root)
    
    def flatten_data(self, element, path=''):
        flat_data = []
        current_path = f"{path}/{element['tag']}"
        
        if 'children' not in element:
            return [{**element['attributes'], 'path': current_path, 'text': element['text']}]
        
        for child in element['children']:
            child_data = self.flatten_data(child, path=current_path)
            flat_data.extend(child_data)
            
        return flat_data
    
        
parser = TestParser('en_product3_181.xml')
parsed_data = parser.parse_root()
flat_data = parser.flatten_data(parsed_data)

df = pd.DataFrame(flat_data)
df.to_excel('output.xlsx', index=False)
df.to_csv('output.csv', index=False)
#here I refined both csv and xlsx formats to compare the differences, if any.


"""
This method is very unideal because it is not scalable. 
Will try to find a better way to parse the XML file.
"""