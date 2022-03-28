def is_bool(key):
    return isinstance(key, bool)

def is_number(key):
    return isinstance(key, (int, float))

def is_list(key):
    return isinstance(key, list)

def is_string(key):
    return isinstance(key, str)

def is_str_num(key):
    return is_number(key) or is_string(key)

# def is_list_of_string(key):
#     if is_list(key):
#         return all([is_string(i) for i in key])
#     return False

# def is_list_of_number(key):
#     if is_list(key):
#         return all([is_number(i) for i in key])
#     return False

def is_list_of_str_num(key):
    if is_list(key):
        return all([is_str_num(i) for i in key])
    return False