from pathlib import Path
import os

def get_project_root():
    # Returns project root folder.
    return Path(__file__).parent.parent

def validate_envir_variable(var_name):
    out = False
    if os.environ.get(var_name):
        out = True
    return out

def extend_list_with_other_list(list_trg, list_to_add):
    if list_to_add and isinstance(list_to_add, list):
        list_trg.extend(list_to_add)
    return list_trg