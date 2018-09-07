"""
coding=utf-8
"""

import docx2txt
import os
import json

import argparse
import re
import xml.etree.ElementTree as ET
import zipfile
import sys

import spacy
import extract_entities

def qn(tag):
 
    "    Stands for 'qualified name', a utility function to turn a namespace\n",
    "    prefixed tag name into a Clark-notation qualified tag name for lxml. For\n",
    "    example, ``qn('p:cSld')`` returns ``'{http://schemas.../main}cSld'``.\n",
    "    Source: https://github.com/python-openxml/python-docx/\n",

    prefix, tagroot = tag.split(':')
    uri = nsmap[prefix]
    return '{{{}}}{}'.format(uri, tagroot)

def xml2text(xml):

    "    A string representing the textual content of this run, with content\n",
    "    child elements like ``<w:tab/>`` translated to their Python\n",
    "    equivalent.\n",
    "    Adapted from: https://github.com/python-openxml/python-docx/\n",
    "    \"\"\"\n",
    text = u''
    root = ET.fromstring(xml)
    for child in root.iter():
        if child.tag == qn('w:t'):
            t_text = child.text
            text += t_text if t_text is not None else ''
        elif child.tag == qn('w:tab'):
            text += '\t'
        elif child.tag in (qn('w:br'), qn('w:cr')):
            text += '\n'
        elif child.tag == qn("w:p"):
            text += '\n\n'
    return text

nsmap = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
    #there is no header, footer or image in the docx but still keep it --> only for doc_xml\n",

def get_main_text(docx, img_dir=None):
    text = u''

    # unzip the docx in memory
    zipf = zipfile.ZipFile(docx)
    filelist = zipf.namelist()

    # get header text
    # there can be 3 header files in the zip
    header_xmls = 'word/header[0-9]*.xml'
    for fname in filelist:
        if re.match(header_xmls, fname):
            text += xml2text(zipf.read(fname))

    #get main text
    doc_xml = 'word/document.xml'
    text += xml2text(zipf.read(doc_xml))

    # get footer text\n",
    # there can be 3 footer files in the zip\n",
    footer_xmls = 'word/footer[0-9]*.xml'
    for fname in filelist:
        if re.match(footer_xmls, fname):
            text += xml2text(zipf.read(fname))

    if img_dir is not None:
    # extract images\n",
        for fname in filelist:
            _, extension = os.path.splitext(fname)
            if extension in [".jpg", ".jpeg", ".png", ".bmp"]:
                dst_fname = os.path.join(img_dir, os.path.basename(fname))
                with open(dst_fname, "w") as dst_f:
                    dst_f.write(zipf.read(fname))

    zipf.close()
    return text.strip()


def chunk_text(docx, img_dir=None):
    a=get_main_text(docx, img_dir=None)
    b= a.split("\n\n")
    return list(filter(None,b))
    
#def get_text(a):
#    text = docx2txt.process(a)
#    return text
#
#title = os.listdir('../Datablob/')
#text = [get_text('../Datablob/' + filename) for filename in os.listdir('../Datablob/')]
#idb=[]
#for i in range(len(text)):
#    idb.append(i+1)
#    print(idb)
#    print(text)
#
#title_text=[{"id":a,"title": b, "text": c} for a,b,c in zip(idb, title, text)]
##js= json.dumps(title_text)
#doc={}
#doc['documents']= title_text
#json_doc = json.dumps(doc)
#print(json_doc)
#
#text_with_jump = [get_main_text('../Datablob/' + filename) for filename in os.listdir('../Datablob/')]

#chunk the text depending on paragraphs\n",
#chunk_text=[]
#for i in range(len(text_with_jump)):
#    a=text_with_jump[i].split("\n\n")
#    a=list(filter(None, a))
#    chunk_text.append(a)
#    for i in range(len(text_with_jump)):
#        print(len(chunk_text[i]))

#
#nlp=spacy.load('en_core_web_sm')
#for i in range(len(chunk_text[1])):
#    a=extract_entities.entities(nlp(chunk_text[1][i]))
#    print(a)


