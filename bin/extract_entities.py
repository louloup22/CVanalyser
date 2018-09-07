"""
coding=utf-8
"""

import spacy
import logging
import lib
from commonregex import CommonRegex

#EMAIL_REGEX = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}"
EMAIL_REGEX=r"[\w\.-]+@[\w\.-]+"
#PHONE_REGEX = r"\(?(\d{3})?\)?[\s\.-]{0,2}?(\d{3})[\s\.-]{0,2}(\d{4})"
PHONE_REGEX = r'[\+][0-9 \d ()]{4,21}'
BIRTHDATE_REGEX=r'[\d]{1,2}[/\.][\d]{1,2}[/\.][\d]{4}'
UNIT_POSTCODE_REGEX=r'#[-,.\d ()]{4,}'
URL_REGEX=r'https?://[^\s]+'

def entity_extractor(label,input_string, nlp):
    input_string = str(input_string)

    doc = nlp(input_string)
    # Extract entities
    doc_entities = doc.ents
    # Subset to person type entities
    doc_persons = filter(lambda x: x.label_ == label, doc_entities)
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    #create a list instead of map in Python 3
    # Assuming that the first Person entity with more than two tokens is the candidate's name
    #just checking and adding other name of persons found
    if doc_persons:
        return doc_persons
    return "NOT FOUND"

def candidate_name_extractor(input_string, nlp):
    input_string = str(input_string)

    doc = nlp(input_string)
    # Extract entities
    doc_entities = doc.ents
    # Subset to person type entities
    doc_persons = filter(lambda x: x.label_ == 'PERSON', doc_entities)
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    #create a list instead of map in Python 3
    # Assuming that the first Person entity with more than two tokens is the candidate's name
    #just checking and adding other name of persons found
    if doc_persons:
        return doc_persons[0:5]
    return "NOT FOUND"

def nationality_extractor(input_string, nlp):
    input_string = str(input_string)

    doc = nlp(input_string)
    # Extract entities
    doc_entities = doc.ents
    # Subset to person type entities
    doc_persons = filter(lambda x: x.label_ == 'NORP', doc_entities)
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    #create a list instead of map in Python 3
    # Assuming that the first Person entity with more than two tokens is the candidate's name
    #just checking and adding other name of persons found
    if doc_persons:
        return doc_persons[0:5]
    return "NOT FOUND"

#extractors are (in the conf template file): experience, position, platforms, database, programming, machinelearning
    #universities, languages, hobbies, open-source, IT_business_tools, ...
def extract_fields(df):
    for extractor, items_of_interest in lib.get_conf('extractors').items():
        df[extractor] = df['text'].apply(lambda x: extract_skills(x, extractor, items_of_interest))
    return df

def extract_skills(resume_text, extractor, items_of_interest):
    potential_skills_dict = dict()
    matched_skills = set()

    # TODO This skill input formatting could happen once per run, instead of once per observation.
    for skill_input in items_of_interest:

        # Format list inputs
        if type(skill_input) is list and len(skill_input) >= 1:
            potential_skills_dict[skill_input[0]] = skill_input

        # Format string inputs
        elif type(skill_input) is str:
            potential_skills_dict[skill_input] = [skill_input]
        else:
            logging.warn('Unknown skill listing type: {}. Please format as either a single string or a list of strings'
                         ''.format(skill_input))

    for (skill_name, skill_alias_list) in potential_skills_dict.items():

        skill_matches = 0
        # Iterate through aliases
        for skill_alias in skill_alias_list:
            # Add the number of matches for each alias
            skill_matches += lib.term_count(resume_text, skill_alias.lower())

        # If at least one alias is found, add skill name to set of skills
        if skill_matches > 0:
            matched_skills.add(skill_name)

    return matched_skills


def entities(doc):
    tags={}
    for entity in doc.ents:
        print(entity.label, entity.label_, ' '.join(t.orth_ for t in entity))
        term=' '.join(t.orth_ for t in entity)
        if ' '.join(term) not in tags:
            tags[term]=[(entity.label, entity.label_)]
        else:
            tags[term].append((entity.label, entity.label_))
    print(tags)



