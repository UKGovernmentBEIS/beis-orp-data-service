# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 11:38:18 2022

@author: imane.hafnaoui
"""

import json
import sys


def schema2json(path):
    with open(path, "r") as f:
        schema = f.readlines()

    schema = list(
        filter(
            None,
            "".join(
                [
                    i.replace("\n", "").replace("\t", "")
                    for i in schema
                    if not (i.startswith("#") or i.startswith("define"))
                ]
            ).split(";"),
        )
    )

    ent = {}
    rel = {}
    attrs = {}

    for i in schema:
        if "entity" in i:  # entity
            elements = i.split(",")
            name = "unknown"
            roles = []
            attr = []
            ident = None
            for e in elements:
                se = e.strip().split(" ")
                if "sub" in e:
                    name = se[0]
                elif "plays" in e:
                    roles.append(se[-1])
                elif "owns" in e:
                    a, ident = (se[-2], se[-2]) if "@" in e else (se[-1], ident)
                    attr.append(a)
            ent[name] = {"attr": attr, "roles": roles}
            if ident:
                ent[name]["identifier"] = ident
        elif "relates" in i:  # relation
            elements = i.split(",")
            name = "unknown"
            roles = []
            attr = []
            for e in elements:
                se = e.strip().split(" ")
                if "sub" in e:
                    name = se[0]
                elif "relates" in e:
                    roles.append(se[-1])
                elif "owns" in e:
                    attr.append(se[-1])
            rel[name] = {"attr": attr, "links": roles}
        else:  # attr
            elements = i.strip().split(",")
            subclass = (elements[0].split(" ")[-1]).strip()
            # print(elements[0], subclass)
            if subclass in ent.keys():
                name = elements[0].split(" ")[0]
                roles = ent[subclass]["roles"].copy()
                attr = ent[subclass]["attr"].copy()
                #            ident = ent[subclass]['identifier']
                for e in elements[1:]:
                    se = e.strip().split(" ")
                    if "plays" in e:
                        roles.append(se[-1])
                    elif "owns" in e:
                        a, ident = (se[-2], se[-2]) if "@" in e else (se[-1], ident)
                        attr.append(a)
                ent[name] = {"attr": attr, "roles": roles}
                if ident:
                    ent[name]["identifier"] = ident
            elif subclass in rel.keys():
                name = elements[0].split(" ")[0]
                roles = rel[subclass]["links"].copy()
                attr = rel[subclass]["attr"].copy()
                #            ident = ent[subclass]['identifier']
                for e in elements[1:]:
                    se = e.strip().split(" ")
                    if "relates" in e:
                        roles.append(se[-1])
                    elif "owns" in e:
                        attr.append(se[-1])
                rel[name] = {"attr": attr, "links": roles}
            else:
                name = elements[0].split(" ")[0]
                # attr, value = attrs[subclass].copy().values() if subclass in attrs.keys() else ([], 'NA')
                attr = (
                    attrs[subclass]["attr"].copy() if subclass in attrs.keys() else []
                )
                value = attrs[subclass]["value"] if subclass in attrs.keys() else "NA"
                for e in elements[1:]:
                    se = e.strip().split(" ")
                    if "value" in e:
                        value = se[-1]
                    elif "owns" in e:
                        attr.append(se[-1])
                attrs[name] = {"attr": attr, "value": value}

    jsonobj = {"entity": ent, "relation": rel, "attribute": attrs}

    return jsonobj


if __name__ == "__main__":
    args = sys.argv
    schema_file = args[1]
    svfile = args[2]
    with open(svfile, "w+") as f:
        jsobj = schema2json(schema_file)
        json.dump(jsobj, f)
