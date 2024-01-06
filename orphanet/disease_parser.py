import lxml.etree as ET
from dataclasses import dataclass, asdict, field
import h5py
import json
import numpy as np
from typing import List, Generator


@dataclass
class Disease:
    disorder_id: int
    disorder_type: str
    orpha_code: int
    name: str
    expert_link: str
    meta_data: dict = field(default_factory=dict)


class OrphanetDiseaseXMLParser:
    def __init__(
        self, disease_xml_file: str, out_dir: str
    ):  # tree structure of the xml file
        self.xml_file = disease_xml_file
        self.out_dir = out_dir
        self.tree = ET.parse(self.xml_file)
        self.root = self.tree.getroot()
        self.meta_data = self.extract_meta_data()

    def extract_meta_data(self) -> List[str]:
        metadata = []
        elements_of_interest = ["Licence", "ClassificationList"]

        for elem_name in elements_of_interest:
            elem = self.root.find(f".//{elem_name}")
            if elem is not None:
                metadata.extend([elem.findtext(tag, default="Unknown") for tag in elem])

        return metadata

    def parse_disorder(self, elem: ET.Element) -> Disease:
        disorder_id = int(elem.attrib.get("id", 0))
        disorder_type = elem.findtext(".//DisorderType/Name", default="Unknown")
        orpha_code = int(elem.findtext(".//OrphaCode", default="0"))
        name = elem.findtext(".//Name", default="Unknown")
        expert_link = elem.findtext(".//ExpertLink", default="Unknown")

        return Disease(
            disorder_id=disorder_id,
            disorder_type=disorder_type,
            orpha_code=orpha_code,
            name=name,
            expert_link=expert_link,
            meta_data=self.extract_meta_data(),
        )

    def __iter_parse_diseases_xml__(self) -> Generator[Disease, None, None]:
        for event, elem in ET.iterparse(self.xml_file, events=("end",), tag="Disorder"):
            yield self.parse_disorder(elem)
            elem.clear()
