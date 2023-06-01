"""
Simple mapping of PubMed fields to the fields in the Lucene index.
"""

# TODO: There needs to be some more work to restrict certain
#       atoms to fields, e.g., date->dp, but not date->title.

#: The mapping of PubMed fields to the fields in the Lucene index.
mapping = {
    # TODO: At the moment, the fields must be arrays, since
    #       the title and the abstract are separate fields.
    "All Fields": ["all_fields"],
    "all": ["all_fields"],  # Might be unofficial?
    "Title/Abstract": ["title", "abstract"],
    "tiab": ["title", "abstract"],
    "TIAB": ["title", "abstract"],
    "Title": ["title"],
    "ti": ["title"],
    "Abstract": ["abstract"],
    "ab": ["abstract"],

    # TODO: In reality, there are many kinds of MeSH headings
    #       so in the future, we may wish to expand these fields.
    "mh": ["mesh_heading_list"],
    "MeSH": ["mesh_heading_list"],
    "MESH": ["mesh_heading_list"],
    "MeSH Terms": ["mesh_heading_list"],
    "Mesh": ["mesh_heading_list"],
    "Pharmacological Action": ["mesh_heading_list"],  # TODO: This is unlikely correct.

    "nm": ["supplementary_concept_list"],
    "Supplementary Concept": ["supplementary_concept_list"],

    "sh": ["mesh_qualifier_list"],
    "Subheading": ["mesh_qualifier_list"],
    "MeSH Subheading": ["mesh_qualifier_list"],

    "MAJR": ["mesh_major_heading_list"],
    "Majr": ["mesh_major_heading_list"],
    "majr": ["mesh_major_heading_list"],

    "Publication Type": ["publication_type"],
    "pt": ["publication_type"],

    "Keywords": ["keyword_list"],
    "kw": ["keyword_list"],

    # TODO: for now, there is only a single date field,
    #       which corresponds to the publish date in Pubmed.
    "Publication Date": ["date"],
    "dp": ["date"],

    "PMID": ["id"],
    "pmid": ["id"],

    # TODO: No mapping yet. Empty list means the
    #       term is not included in the query.
    "jour": ["publication_type"],

    # TODO: Not sure if this is necessarily correct.
    "tw": ["all_fields"],
    "Text Word": ["all_fields"]
}
