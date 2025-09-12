"""
This module provides the utility functions for cds.
"""

from hana_ml.ml_base import quotename


def type_sql2cds(sql_type):
    """
    For internal use.
    """
    mappings = {
        "BOOLEAN" : "Boolean",
        "TINYINT" : "UInt8",
        "SMALLINT" : "Int16",
        "INTEGER" : "Integer",
        "BIGINT" : "Int64",
        "INT" : "Integer",
        "DECIMAL" : "Decimal",
        "DOUBLE" : "Double",
        "DATE" : "Date",
        "TIMESTAMP" : "Timestamp",
        "TIME" : "Time",
        "NVARCHAR" : "String",
        "VARBINARY" : "Binary",
        "VARCHAR" : "String",
        "NCLOB" : "LargeString",
        "BOLB" : "LargeBinary"
    }
    for kkey, vval in mappings.items():
        if kkey in sql_type.upper():
            return sql_type.upper().replace(kkey, vval)

def create_cds_artifacts(data, namespace, context, entity, primary_key=None, create_view=True, annotation=None, hdbtable_name=None, hdbview_name=None):
    """
    For internal use.
    """
    anno_prefix = ''
    if annotation:
        anno_prefix = "\n  ".join(annotation) + '\n'
    if hdbtable_name is None:
        hdbtable_name = "{}_{}_{}".format(namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
    if hdbview_name is None:
        hdbview_name = "{}_{}_{}".format(namespace.replace(".", "_").upper(), context.replace(".", "_").upper(), entity.replace(".", "_").upper())
    table_struct = data.get_table_structure()
    hdbtable_content = []
    hdbview_content = []
    cds_content = []
    hdbtable_primary_key = ''
    max_length = 0
    for kkey in table_struct:
        if max_length < len(kkey):
            max_length = len(kkey)
    if primary_key:
        for kkey, vval in table_struct.items():
            mod_kkey = ''.join(elem for elem in kkey.lower() if (elem.isalnum() or elem=='_'))
            space = " " * (max_length - len(kkey) + 1)
            mod_space = " " * (max_length - len(mod_kkey) + 1)
            if kkey == primary_key:
                hdbtable_primary_key = "  primary key ({})".format(quotename(kkey))
                cds_content.append("    key {}{} : {};".format(mod_kkey, mod_space, type_sql2cds(vval)))
            else:
                cds_content.append("        {}{} : {};".format(mod_kkey, mod_space, type_sql2cds(vval)))
            hdbtable_content.append("  {} {}".format(quotename(kkey), vval.lower()))
            hdbview_content.append("  {}{} AS {}".format(quotename(kkey), space, mod_kkey.upper()))
        hdbtable_content.append(hdbtable_primary_key)
    else:
        for kkey, vval in table_struct.items():
            mod_kkey = ''.join(elem for elem in kkey.lower() if (elem.isalnum() or elem=='_'))
            space = " " * (max_length - len(kkey) + 1)
            mod_space = " " * (max_length - len(mod_kkey) + 1)
            cds_content.append("    {}{} : {};".format(mod_kkey, mod_space, type_sql2cds(vval)))
            hdbtable_content.append("  {} {}".format(quotename(kkey), vval.lower()))
            hdbview_content.append("  {}{} AS {}".format(quotename(kkey), space, mod_kkey.upper()))
    hdbtable = "COLUMN TABLE {} (\n{}\n)".format(quotename(hdbtable_name), ",\n".join(hdbtable_content))
    hdbview = "VIEW {} AS SELECT\n{}\nFROM {}".format(hdbview_name, ",\n".join(hdbview_content), quotename(hdbtable_name))
    cds = "  {}  entity {} {{\n{}\n  }}".format(anno_prefix, entity, "\n".join(cds_content))
    if create_view:
        return hdbtable, hdbview, cds
    else:
        return hdbtable.replace('"', ""), cds
