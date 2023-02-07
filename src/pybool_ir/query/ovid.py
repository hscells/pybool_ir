"""
Convert OVID query to single-line PubMed query

This code is unlicensed, but should be attributed to
https://github.com/Amal-Alharbi/OVID_TO_PUBMED

The original author of this code is Amal Alharbi.

Only very minor, superficial function naming changes
have been applied to this file. It is mostly original.
"""

# TODO: This code should be replaced with a proper parser of the likes of the Pubmed query parser.

import re

from pybool_ir.query import PubmedQueryParser


def _Convert_OVID_To_PUBMED(q_OVID):
    SubHeading_Dict = {}
    SubHeading_list = ["Abnormalities", "Administration and Dosage", "Adverse Effects", "Agonists", "Analogs and Derivatives", "Analysis", "Anatomy and Histology", "Antagonists and Inhibitors", "Biosynthesis", "Blood Supply", "Blood", "Cerebrospinal Fluid", "Chemical Synthesis", "Chemically Induced", "Chemistry", "Classification", "Complications", "Congenital", "Contraindications", "Cytology", "Deficiency", "Diagnosis", "Diagnostic Use", "Diet Therapy", "Diagnostic Imaging", "Drug Effects", "Drug Therapy", "Economics", "Education", "Embryology", "Enzymology", "Epidemiology", "Ethics", "Ethnology", "Etiology", "Genetics", "Growth and Development", "History", "Immunology", "Injuries", "Innervation", "Instrumentation", "Isolation and Purification", "Legislation and Jurisprudence", "Manpower", "Metabolism", "Methods", "Microbiology", "Mortality", "Nursing", "Organization and Administration", "Parasitology", "Pathogenicity", "Pathology", "Pharmacokinetics", "Pharmacology", "Physiology",
                       "Physiopathology", "Poisoning", "Prevention and Control", "Psychology", "Radiation Effects", "Radiotherapy", "Rehabilitation", "Secondary", "Secretion", "Standards", "Statistics and Numerical Data", "Supply and Distribution", "Surgery", "Therapeutic Use", "Therapy", "Toxicity", "Transmission", "Transplantation", "Trends", "Ultrastructure", "Urine", "Utilization", "Veterinary", "Virology"]
    SubHeading_Ab = ["AB", "AD", "AE", "AG", "AA", "AN", "AH", "AI", "BI", "BS", "BL", "CF", "CS", "CI", "CH", "CL", "CO", "CN", "CT", "CY", "DF", "DI", "DU", "DH", "DG", "DE", "DT", "EC", "ED", "EM", "EN", "EP", "ES", "EH", "ET", "GE", "GD", "HI", "IM", "IN", "IR", "IS", "IP", "LJ", "MA", "ME", "MT", "MI", "MO", "NU", "OG", "PS", "PY", "PA", "PK", "PD", "PH", "PP", "PO", "PC", "PX", "RE", "RT", "RH", "SC", "SE", "ST", "SN", "SD", "SU", "TU", "TH", "TO", "TM", "TR", "TD", "UL", "UR", "UT", "VE", "VI"]
    SubHeading_Explode = ["AN", "AH", "CY", "EM", "CH", "DI", "ET", "CO", "MI", "OG", "PD", "AE", "PH", "ME", "SN", "EP", "TU", "TH", "SU"]

    # create a dictionary containes SubHeading Abbreviation:SubHeading
    for i, item in enumerate(SubHeading_Ab):
        SubHeading_Dict[SubHeading_Ab[i]] = SubHeading_list[i]

    # remove empty lines
    for i, q in enumerate(q_OVID):
        q_OVID[i] = q_OVID[i].replace('\x00', '')
    q_OVID = list(filter(None, q_OVID))

    # replace OVID operators with Pubmed operators
    process = []
    operations = [' and ', ' or ', ' not ']

    for line in q_OVID:
        temp = line.strip()
        # convert operation to PubMed format
        temp = temp.replace('.pt.', '[Publication Type]').replace('.ab,ti.', '[Title/Abstract]').replace('.tw.', '[Text Word]').replace('.fs.', '[sh:noexp]').replace('.sh.', '[mh:noexp]').replace('.ti,ab.', '[Title/Abstract]').replace('.ti.', '[ti]').replace('$', '*').replace('.mp.', '[all]').replace('.mp', '[all]').replace('.ab.', '[Title/Abstract]').replace('.ti,ab,sh.', '[all]').replace('.ed,dc.', '[Date - Completion]').replace('.hw.', '[mh]').replace('[mp=title, original title, abstract, name of substance word, subject heading word]', '')

        # remove number of hits from the query line
        check_hits = re.findall(r'\([0-9]+\)', temp)
        if check_hits:
            temp = temp.replace(check_hits[0], '')

        # remove line number
        check_line = re.findall(r'^\d+\s+[a-zA-Z]+\s+\d+', temp)
        check_line_no = re.findall(r'^[0-9]+\.|^[0-9]+\s+\D+|^[0-9]+\s+\d+\s+((and)|(or)|(not))\s+\d+|^#*[0-9]+\s+#*\d+\s+((and)|(or)|(not))\s+#*\d+|^#+\d+\s+|^\d+\s+\d+\s+', temp)
        if check_line_no and not check_line:
            temp = temp.split()[1:]
            temp = ' '.join(temp)

        # check if line containes multible operations
        flag = 0
        for oper in operations:
            if oper in temp:
                flag = 1

        if flag == 1:  # if line containes multible operations
            terms = []
            op_order = []

            for term in temp.split():
                if (' ' + term + ' ') in operations:
                    op_order.append(term)

            for term in re.split(r' or | not | and ', temp):
                check = re.findall(r'\/[a-zA-Z]+', term)
                # find subeadding abbreviation
                if check and (len(check[0]) == 3) and check[0].lower() != '/or':
                    meshs_names = re.findall(r'\[+\D+\]+', term.split('/')[1])
                    if meshs_names:
                        meshs = (term.split('/')[1]).replace(meshs_names[1], '')
                        term = term.replace(meshs_names[1], '')
                    else:
                        meshs = term.split('/')[1]

                    mesh_set = meshs.split(',')

                    temp_sub = ''

                    for i in range(len(mesh_set)):
                        abbr = mesh_set[i].replace(')', '').strip()
                        mesh = SubHeading_Dict[abbr.upper()]
                        if 'exp ' in term:
                            term_ = '"' + term.replace(meshs, mesh + '"[MeSH]').replace('exp ', '')

                        else:
                            term_ = '"' + term.replace(meshs, mesh + '"[mesh:noexp]')

                        if i == 0:
                            temp_sub = term_
                        else:
                            temp_sub = temp_sub + ' OR ' + term_

                    term = temp_sub

                else:
                    if '/' and 'exp ' in term:
                        term = term.replace('exp ', '').replace('/', '[MeSH]')

                    elif '/' and '*' in term and '[Title/Abstract]' not in term:
                        term = term.replace('/', '[MeSH Major Topic]')

                    elif '/' in term and '[Title/Abstract]' not in term and 'or/' not in term.lower() and '/or' not in term.lower() and 'and/' not in term.lower():
                        term = term.replace('/', '[Mesh:NoExp]')

                terms.append(term.strip())

            q = ''
            for i, term in enumerate(terms):
                if i < len(op_order):
                    q = q + ' ' + term + ' ' + op_order[i]
                else:
                    q = q + ' ' + term

            # replace adj with and operator
            q = re.sub(r'adj\d*', 'and', q)
            process.append('(' + q.strip() + ' )')

            # *******************************************************************
        else:
            # check if there is a subheading
            check = re.findall(r'\/[a-zA-Z]+', temp)
            if check and (len(check[0]) == 3) and check[0].lower() != '/or':
                meshs_names = re.findall(r'\[+\D+\]+', temp.split('/')[1])
                if meshs_names:
                    meshs = (temp.split('/')[1]).replace(meshs_names[0], '')
                    temp = temp.replace(meshs_names[0], '')
                else:
                    meshs = temp.split('/')[1]

                mesh_set = meshs.split(',')

                temp_sub = ''

                if (len(mesh_set)) > 1:
                    for i in range(len(mesh_set)):
                        abbr = mesh_set[i].replace(')', '').strip()
                        mesh = SubHeading_Dict[abbr.upper()]
                        if 'exp ' in temp:
                            term_ = '"' + temp.replace(meshs, mesh + '"[MeSH]').replace('exp ', '')

                        else:
                            term_ = '"' + temp.replace(meshs, mesh + '"[mesh:noexp]')

                        if i == 0:
                            temp_sub = term_
                        else:
                            temp_sub = temp_sub + ' OR ' + term_
                else:
                    if 'exp ' in temp:
                        temp_sub = '"' + temp.replace(meshs, SubHeading_Dict[meshs.split()[0].upper()] + '"[MeSH]').replace('exp ', '')
                    else:
                        temp_sub = '"' + temp.replace(meshs, SubHeading_Dict[meshs.split()[0].upper()] + '"[mesh:noexp]')

                # replace adj with and operator
                temp_sub = re.sub(r'adj\d*', 'and', temp_sub)
                process.append('( ' + temp_sub.strip() + ' )')

            else:
                if '/' and 'exp ' in temp:
                    temp = temp.replace('exp ', '').replace('/', '[MeSH]')
                elif '/' and '*' in temp and '[Title/Abstract]' not in temp:
                    temp = temp.replace('/', '[MeSH Major Topic]')
                elif '/' in temp and '[Title/Abstract]' not in temp and 'or/' not in temp.lower() and '/or' not in temp.lower() and 'and/' not in temp.lower():
                    temp = temp.replace('/', '[Mesh:NoExp]')

                # replace adj with and operator
                temp = re.sub(r'adj\d*', 'and', temp)
                process.append('( ' + temp.strip() + ' )')

    return process


# ******************************************************************************

def _convert_to_one_line(query):
    q = {}

    for i, line in enumerate(query.split('\n')):
        q[i + 1] = line

    # find combined operations or/1-3 convert to 1 or 2 or 3
    for key, value in q.items():
        temp = value.lower().replace('(', '').replace(')', '').strip()
        multi = 0
        if re.findall(r'^or+\/+\d+-+\d+\s+(and+|not+)', temp):
            multi = 1
            opemul = re.findall(r'^or+\/+\d+-+\d+\s+(and+|not+)', temp)

        if multi == 1:
            parts = re.split(r' not | and ', temp)
            q_ = ''
            for part in parts:
                if re.findall(r'^or+\/+\d+-+\d+', part):
                    oper = part.split('/')[0]
                    start = int((part.split('/')[1]).split('-')[0])
                    end = int(part.split('-')[1])

                    part_q = ''
                    while start <= end:
                        if start == end:
                            part_q = part_q + ' ' + str(start)
                        else:
                            part_q = part_q + ' ' + str(start) + ' ' + oper
                        start += 1

                elif re.findall(r'^(or|and)+\/+\d+,+\d+', part):
                    oper = part.split('/')[0]
                    part_q = part.split('/')[1].replace(',', ' ' + oper + ' ')

                if q_ == '':
                    q_ = part_q + ' ' + opemul[0] + ' '
                else:
                    q_ = q_ + part_q

            q[key] = '( ' + q_ + ' )'


        elif re.findall(r'^or+\/+\d+(-|‐)+\d+', temp):
            oper = temp.split('/')[0]
            start = int((temp.split('/')[1]).split('-')[0])
            end = int(temp.split('-')[1])

            temp_q = ''
            while start <= end:
                if start == end:
                    temp_q = temp_q + ' ' + str(start)
                else:
                    temp_q = temp_q + ' ' + str(start) + ' ' + oper
                start += 1

            q[key] = '( ' + temp_q + ' )'


        elif re.findall(r'^(or|and)+\/+\d+,+\d+', temp):
            oper = temp.split('/')[0]
            temp_q = temp.split('/')[1].replace(',', ' ' + oper + ' ')
            q[key] = '( ' + temp_q + ' )'

    operations = ['and', 'or', 'not']

    for key, value in q.items():
        terms = value.replace('(', '').replace(')', '').split()
        temp = value.replace('(', '').replace(')', '')

        # replace each number with the equivalent line
        for j, term in enumerate(terms):
            if terms[j].isdigit() and (terms[j - 1].lower() in operations or terms[j + 1].lower() in operations):
                index = terms[j]
                temp = temp.replace(str(index), q[int(index)])
        temp = temp.replace('1(', '(')

        q[key] = '( ' + temp + ' )'

    return q


# ******************************************************************************
# ******************************************************************************

def transform(query: str):
    parser = PubmedQueryParser()
    q_OVID = query.split('\n')
    q_con = _Convert_OVID_To_PUBMED(q_OVID)
    q_Pub = _convert_to_one_line('\n'.join(q_con))
    if 'limit' in list(q_Pub.values())[-1]:
        return str(parser.parse_ast(list(q_Pub.values())[-2]))
    else:
        return str(parser.parse_ast(list(q_Pub.values())[-1]))

# ******************************************************************************
# ******************************************************************************
