# TODO: There needs to be some more work to restrict certain
#       atoms to fields, e.g., date->dp, but not date->title.

mapping = {
    # TODO: At the moment, the fields must be arrays, since
    #       the title and the abstract are separate fields.
    "Title/Abstract": ["title", "abstract"],
    "tiab": ["title", "abstract"],
    "Title": ["title"],
    "ti": ["title"],
    "Abstract": ["abstract"],
    "ab": ["abstract"],

    # TODO: In reality, there are many kinds of MeSH headings
    #       so in the future, we may wish to expand these fields.
    "MeSH": ["mesh_heading_list"],
    "Mesh": ["mesh_heading_list"],
    "sh": ["mesh_heading_list"],

    "Publication Type": ["publication_type"],
    "pt": ["publication_type"],

    "Keywords": ["keyword_list"],
    "kw": ["keyword_list"],

    # TODO: for now, there is only a single date field,
    #       which corresponds to the publish date in Pubmed.
    "Publication Date": ["date"],
    "dp": ["date"],

    "PMID": ["pmid"],
    "pmid": ["pmid"],
}
