# from http.client import HTTPSConnection
# import requests
# url = 'http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest'
# res = requests.get(url)
# with open('../../all.xml', 'w', encoding='utf-8') as f:
#     f.write(res.text)
from lxml.etree import parse
# from xml.etree.ElementTree import parse

tree = parse("../../all.xml")
root = tree.getroot()
print(root)
# df_list = root.findall('str:Dataflow', {'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'})
df_list = root.findall('.//mes:Structures/str:Dataflows/str:Dataflow', root.nsmap)
for it in df_list:

    print(it.attrib['id'])

