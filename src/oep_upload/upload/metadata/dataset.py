from __future__ import annotations
from copy import deepcopy
from typing import Any

from oemetadata.v2.v20.example import OEMETADATA_V20_EXAMPLE
from oemetadata.v2.v20.template import OEMETADATA_V20_TEMPLATE


# map a frictionless pkg to dataset-level OEMetadata (no `resources`)
def assemble_dataset_metadata_from_pkg(pkg: dict[str, Any]) -> dict[str, Any]:
    md = deepcopy(OEMETADATA_V20_TEMPLATE)
    md["@context"] = OEMETADATA_V20_EXAMPLE["@context"]
    md["resources"] = []  # IMPORTANT: dataset doc has no resources

    # basic requireds (adapt if your validated_data already provides these)
    md["@id"] = pkg.get("id") or pkg.get("@id")  # tolerate both
    md["name"] = pkg.get("name")
    md["title"] = pkg.get("title") or pkg.get("name")
    md["description"] = pkg.get("description")

    # optional: carry over common fields if present on the datapackage
    # (oemetadata keys may differ; adapt/extend as you need)
    if "keywords" in pkg:
        md["keywords"] = pkg["keywords"]
    if "license" in pkg:
        # frictionless can be single object or list; normalize to list of strings/objects
        md["license"] = pkg["license"]

    # Examples of optional mappings you might enable:
    # md["publisher"] = ...
    # md["contributors"] = ...
    # md["temporal"] = ...
    # md["spatial"] = ...

    return md
